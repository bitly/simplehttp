#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <stddef.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <inttypes.h>
#include <tcutil.h>
#include <tcadb.h>
#include <simplehttp/queue.h>
#include <simplehttp/simplehttp.h>
#include <json/json.h>
#include "pcre.h"

#define NAME                    "simplememdb"
#define VERSION                 "1.3"
#define BUFFER_SZ               1048576
#define SM_BUFFER_SZ            4096

void set_dump_timer();
void fwmatch_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void fwmatch_int_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void del_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void put_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void get_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void get_int_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void incr_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void vanish_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void stats_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void exit_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void usage();
void info();

static TCADB *adb;
static int is_currently_dumping = 0;
static struct event ev;
static pcre *dump_regex;

void finalize_json(struct evhttp_request *req, struct evbuffer *evb, struct evkeyvalq *args, struct json_object *jsobj)
{
    const char *json, *jsonp;
    
    jsonp = (char *)evhttp_find_header(args, "jsonp");
    json = (char *)json_object_to_json_string(jsobj);
    if (jsonp) {
        evbuffer_add_printf(evb, "%s(%s)\n", jsonp, json);
    } else {
        evbuffer_add_printf(evb, "%s\n", json);
    }
    json_object_put(jsobj); // Odd free function
}

void db_error_to_json(const char *err, struct json_object *jsobj)
{
    fprintf(stderr, "error: %s\n", err);
    json_object_object_add(jsobj, "status", json_object_new_string("error"));
    json_object_object_add(jsobj, "message", json_object_new_string(err));
}

void argtoi(struct evkeyvalq *args, char *key, int *val, int def)
{
    char *tmp;
    
    *val = def;
    tmp = (char *)evhttp_find_header(args, (const char *)key);
    if (tmp) {
        *val = atoi(tmp);
    }
}

void fwmatch_int_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char *key, *kbuf, *format;
    int *value;
    int i, max, off, len, n;
    TCLIST *keylist = NULL;
    struct evkeyvalq args;
    struct json_object *jsobj = NULL, *jsobj2, *jsarr = NULL;
    int txt_format = 0;
    int status_code = 200;
    char *status_txt = "OK";
    
    evhttp_parse_query(req->uri, &args);
    key = (char *)evhttp_find_header(&args, "key");
    format = (char *)evhttp_find_header(&args, "format");
    argtoi(&args, "max", &max, 1000);
    argtoi(&args, "length", &len, 10);
    argtoi(&args, "offset", &off, 0);
    if (key == NULL) {
        evhttp_send_error(req, 400, "INVALID_ARG_KEY");
        evhttp_clear_headers(&args);
        return;
    }
    
    if (format && !strncmp(format, "txt", 3)) {
        txt_format = 1;
    }
    
    if (!txt_format) {
        jsobj = json_object_new_object();
        jsarr = json_object_new_array();
    }
    
    keylist = tcadbfwmkeys(adb, key, strlen(key), max);
    for (i=off; keylist!=NULL && i<(len+off) && i<tclistnum(keylist); i++){
      kbuf = (char *)tclistval2(keylist, i);
      value = (int *)tcadbget(adb, kbuf, strlen(kbuf), &n);
      if (value) {
          if (!txt_format) {
              jsobj2 = json_object_new_object();
              json_object_object_add(jsobj2, kbuf, json_object_new_int((int)*value));
              json_object_array_add(jsarr, jsobj2);
          } else {
              evbuffer_add_printf(evb, "%s,%d\n", kbuf, (int)*value);
          }
          tcfree(value);
      }
    }
    
    if (!txt_format) {
        json_object_object_add(jsobj, "results", jsarr);
    }
    
    if (keylist != NULL) {
        tcfree(keylist);
        if (!txt_format) {
            json_object_object_add(jsobj, "status", json_object_new_string("ok"));
        }
    } else {
        status_code = 500;
        status_txt = "INTERNAL_ERROR";
        if (!txt_format) {
            db_error_to_json("INTERNAL_ERROR", jsobj);
        } else {
            evbuffer_add_printf(evb, "INTERNAL_ERROR\n");
        }
    }
    
    if (!txt_format) {
        finalize_json(req, evb, &args, jsobj);
    }
    
    evhttp_send_reply(req, status_code, status_txt, evb);
    evhttp_clear_headers(&args);
}

void fwmatch_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char *key, *kbuf, *value, *format;
    int i, max, off, len, n;
    TCLIST *keylist = NULL;
    struct evkeyvalq args;
    struct json_object *jsobj = NULL, *jsobj2, *jsarr = NULL;
    int txt_format = 0;
    int status_code = 200;
    char *status_txt = "OK";
    
    evhttp_parse_query(req->uri, &args);
    
    key = (char *)evhttp_find_header(&args, "key");
    format = (char *)evhttp_find_header(&args, "format");
    
    argtoi(&args, "max", &max, 1000);
    argtoi(&args, "length", &len, 10);
    argtoi(&args, "offset", &off, 0);
    
    if (key == NULL) {
        evhttp_send_error(req, 400, "INVALID_ARG_KEY");
        evhttp_clear_headers(&args);
        return;
    }
    
    if (format && !strncmp(format, "txt", 3)) {
        txt_format = 1;
    }
    
    if (!txt_format) {
        jsobj = json_object_new_object();
        jsarr = json_object_new_array();
    }
    
    keylist = tcadbfwmkeys(adb, key, strlen(key), max);
    for (i=off; keylist!=NULL && i<(len+off) && i<tclistnum(keylist); i++) {
        kbuf = (char *)tclistval2(keylist, i);
        value = tcadbget(adb, kbuf, strlen(kbuf), &n);
        if (value) {
            if (!txt_format) {
                jsobj2 = json_object_new_object();
                json_object_object_add(jsobj2, kbuf, json_object_new_string(value));
                json_object_array_add(jsarr, jsobj2);
            } else {
                evbuffer_add_printf(evb, "%s,%s\n", kbuf, value);
            }
            tcfree(value);
        }
    }
    
    if (!txt_format) {
        json_object_object_add(jsobj, "results", jsarr);
    }
    
    if (keylist != NULL) {
        tcfree(keylist);
        if (!txt_format) {
            json_object_object_add(jsobj, "status", json_object_new_string("ok"));
        }
    } else {
        status_code = 500;
        status_txt = "INTERNAL_ERROR";
        if (!txt_format) {
            db_error_to_json("INTERNAL_ERROR", jsobj);
        } else {
            evbuffer_add_printf(evb, "INTERNAL_ERROR\n");
        }
    }
    
    if (!txt_format) {
        finalize_json(req, evb, &args, jsobj);
    }
    
    evhttp_send_reply(req, status_code, status_txt, evb);
    evhttp_clear_headers(&args);
}

void del_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key, *format;
    struct evkeyvalq    args;
    struct json_object  *jsobj = NULL;
    int txt_format = 0;
    int status_code = 200;
    char *status_txt = "OK";
    
    evhttp_parse_query(req->uri, &args);
    
    key = (char *)evhttp_find_header(&args, "key");
    format = (char *)evhttp_find_header(&args, "format");
    
    if (key == NULL) {
        evhttp_send_error(req, 400, "INVALID_ARG_KEY");
        evhttp_clear_headers(&args);
        return;
    }
    
    if (format && !strncmp(format, "txt", 3)) {
        txt_format = 1;
    }
    
    if (!txt_format) {
        jsobj = json_object_new_object();
    }
    
    if (tcadbout(adb, key, strlen(key))) {
        if (!txt_format) {
            json_object_object_add(jsobj, "status", json_object_new_string("ok"));
        } else {
            evbuffer_add_printf(evb, "OK\n");
        }
    } else {
        status_code = 500;
        status_txt = "INTERNAL_ERROR";
        if (!txt_format) {
            db_error_to_json("INTERNAL_ERROR", jsobj);
        } else {
            evbuffer_add_printf(evb, "INTERNAL_ERROR\n");
        }
    }
    
    if (!txt_format) {
        finalize_json(req, evb, &args, jsobj);
    }
    
    evhttp_send_reply(req, status_code, status_txt, evb);
    evhttp_clear_headers(&args);
}

void put_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key, *value, *format;
    struct evkeyvalq    args;
    struct json_object  *jsobj = NULL;
    int txt_format = 0;
    int status_code = 200;
    char *status_txt = "OK";
    
    evhttp_parse_query(req->uri, &args);
    
    key = (char *)evhttp_find_header(&args, "key");
    value = (char *)evhttp_find_header(&args, "value");
    format = (char *)evhttp_find_header(&args, "format");
    
    if (key == NULL) {
        evhttp_send_error(req, 400, "INVALID_ARG_KEY");
        evhttp_clear_headers(&args);
        return;
    }
    
    if (value == NULL) {
        evhttp_send_error(req, 400, "INVALID_ARG_VALUE");
        evhttp_clear_headers(&args);
        return;
    }
    
    if (format && !strncmp(format, "txt", 3)) {
        txt_format = 1;
    }
    
    if (!txt_format) {
        jsobj = json_object_new_object();
    }
    
    jsobj = json_object_new_object();
    if (tcadbput(adb, key, strlen(key), value, strlen(value))) {
        if (!txt_format) {
            json_object_object_add(jsobj, "status", json_object_new_string("ok"));
        } else {
            evbuffer_add_printf(evb, "OK\n");
        }
    } else {
        status_code = 500;
        status_txt = "INTERNAL_ERROR";
        if (!txt_format) {
            db_error_to_json("INTERNAL_ERROR", jsobj);
        } else {
            evbuffer_add_printf(evb, "INTERNAL_ERROR\n");
        }
    }
    
    if (!txt_format) {
        finalize_json(req, evb, &args, jsobj);
    }
    
    evhttp_send_reply(req, status_code, status_txt, evb);
    evhttp_clear_headers(&args);
}

void get_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char *key, *value, *format;
    struct evkeyvalq args;
    struct json_object *jsobj = NULL;
    int n;
    int txt_format = 0;
    int status_code = 200;
    char *status_txt = "OK";
    
    evhttp_parse_query(req->uri, &args);
    
    key = (char *)evhttp_find_header(&args, "key");
    format = (char *)evhttp_find_header(&args, "format");
    
    if (key == NULL) {
        evhttp_send_error(req, 400, "INVALID_ARG_KEY");
        evhttp_clear_headers(&args);
        return;
    }
    
    if (format && !strncmp(format, "txt", 3)) {
        txt_format = 1;
    }
    
    if (!txt_format) {
        jsobj = json_object_new_object();
    }
    
    if ((value = tcadbget(adb, key, strlen(key), &n))) {
        if (!txt_format) {
            json_object_object_add(jsobj, "status", json_object_new_string("ok"));
            json_object_object_add(jsobj, "value", json_object_new_string(value));
        } else {
            evbuffer_add_printf(evb, "%s\n", value);
        }
        free(value);
    } else {
        status_code = 500;
        status_txt = "INTERNAL_ERROR";
        if (!txt_format) {
            db_error_to_json("INTERNAL_ERROR", jsobj);
        } else {
            evbuffer_add_printf(evb, "INTERNAL_ERROR\n");
        }
    }
    
    if (!txt_format) {
        finalize_json(req, evb, &args, jsobj);
    }
    
    evhttp_send_reply(req, status_code, status_txt, evb);
    evhttp_clear_headers(&args);
}

void get_int_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char *key, *format;
    int *value;
    struct evkeyvalq args;
    struct json_object *jsobj = NULL;
    int n;
    int txt_format = 0;
    int status_code = 200;
    char *status_txt = "OK";
    
    evhttp_parse_query(req->uri, &args);
    
    key = (char *)evhttp_find_header(&args, "key");
    format = (char *)evhttp_find_header(&args, "format");
    
    if (key == NULL) {
        evhttp_send_error(req, 400, "INVALID_ARG_KEY");
        evhttp_clear_headers(&args);
        return;
    }
    
    if (format && !strncmp(format, "txt", 3)) {
        txt_format = 1;
    }
    
    if (!txt_format) {
        jsobj = json_object_new_object();
    }
    
    if ((value = (int *)tcadbget(adb, key, strlen(key), &n))) {
        if (!txt_format) {
            json_object_object_add(jsobj, "status", json_object_new_string("ok"));
            json_object_object_add(jsobj, "value", json_object_new_int((int)*value));
        } else {
            evbuffer_add_printf(evb, "%d\n", (int)*value);
        }
        free(value);
    } else {
        status_code = 500;
        status_txt = "INTERNAL_ERROR";
        if (!txt_format) {
            db_error_to_json("INTERNAL_ERROR", jsobj);
        } else {
            evbuffer_add_printf(evb, "INTERNAL_ERROR\n");
        }
    }
    
    if (!txt_format) {
        finalize_json(req, evb, &args, jsobj);
    }
    
    evhttp_send_reply(req, status_code, status_txt, evb);
    evhttp_clear_headers(&args);
}

void mget_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char *key, *value, *format;
    struct evkeyvalq args;
    struct evkeyval *pair;
    struct json_object *jsobj = NULL;
    int nkeys = 0;
    int n;
    int txt_format = 0;
    int status_code = 200;
    char *status_txt = "OK";
    
    evhttp_parse_query(req->uri, &args);
    
    format = (char *)evhttp_find_header(&args, "format");
    
    if (format && !strncmp(format, "txt", 3)) {
        txt_format = 1;
    }
    
    if (!txt_format) {
        jsobj = json_object_new_object();
    }
    
    TAILQ_FOREACH(pair, &args, next) {
        if (pair->key[0] != 'k') continue;
        key = (char *)pair->value;
        nkeys++;
        
        value = tcadbget(adb, key, strlen(key), &n);
        if (value) {
            if (!txt_format) {
                json_object_object_add(jsobj, key, json_object_new_string(value));
            } else {
                evbuffer_add_printf(evb, "%s,%s\n", key, value);
            }
        }
        free(value);
    }
    
    if (!nkeys) {
        evhttp_send_error(req, 400, "INVALID_ARG_KEY");
        evhttp_clear_headers(&args);
        return;
    }
    
    if (!txt_format) {
        finalize_json(req, evb, &args, jsobj);
    }
    
    evhttp_send_reply(req, status_code, status_txt, evb);
    evhttp_clear_headers(&args);
}

void mget_int_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char *key, *format;
    int *value;
    struct evkeyvalq args;
    struct evkeyval *pair;
    struct json_object *jsobj = NULL;
    int nkeys = 0;
    int n;
    int txt_format = 0;
    int status_code = 200;
    char *status_txt = "OK";
    
    evhttp_parse_query(req->uri, &args);
    
    format = (char *)evhttp_find_header(&args, "format");
    
    if (format && !strncmp(format, "txt", 3)) {
        txt_format = 1;
    }
    
    if (!txt_format) {
        jsobj = json_object_new_object();
    }
    
    TAILQ_FOREACH(pair, &args, next) {
        if (pair->key[0] != 'k') continue;
        key = (char *)pair->value;
        nkeys++;
        
        value = (int *)tcadbget(adb, key, strlen(key), &n);
        if (value) {
            if (!txt_format) {
                json_object_object_add(jsobj, key, json_object_new_int((int)*value));
            } else {
                evbuffer_add_printf(evb, "%s,%d\n", key, (int)*value);
            }
        }
        free(value);
    }
    
    if (!nkeys) {
        evhttp_send_error(req, 400, "INVALID_ARG_KEY");
        evhttp_clear_headers(&args);
        return;
    }
    
    if (!txt_format) {
        finalize_json(req, evb, &args, jsobj);
    }
    
    evhttp_send_reply(req, status_code, status_txt, evb);
    evhttp_clear_headers(&args);
}

void incr_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char *incr_value, *format;
    struct json_object *jsobj = NULL;
    struct evkeyvalq args;
    int v;
    int value = 1;
    struct evkeyval *arg;
    bool has_key_arg = false;
    bool error = false;
    int txt_format = 0;
    int status_code = 200;
    char *status_txt = "OK";
    
    evhttp_parse_query(req->uri, &args);
    
    incr_value = (char *)evhttp_find_header(&args, "value");
    format = (char *)evhttp_find_header(&args, "format");
    
    if (incr_value != NULL) {
        value = atoi(incr_value);
    }
    
    if (format && !strncmp(format, "txt", 3)) {
        txt_format = 1;
    }
    
    if (!txt_format) {
        jsobj = json_object_new_object();
    }
    
    TAILQ_FOREACH(arg, &args, next) {
        if (strcasecmp(arg->key, "key") == 0) {
            has_key_arg = true;
            if ((v = tcadbaddint(adb, arg->value, strlen(arg->value), value)) == INT_MIN) {
                error = true;
                break;
            }
        }
    }
    
    if (!has_key_arg) {
        evhttp_send_error(req, 400, "INVALID_ARG_KEY");
        evhttp_clear_headers(&args);
        json_object_put(jsobj);
        return;
    }
    
    if (!error) {
        if (!txt_format) {
            json_object_object_add(jsobj, "status", json_object_new_string("ok"));
        } else {
            evbuffer_add_printf(evb, "OK\n");
        }
    } else {
        status_code = 500;
        status_txt = "INTERNAL_ERROR";
        if (!txt_format) {
            db_error_to_json("INTERNAL_ERROR", jsobj);
        } else {
            evbuffer_add_printf(evb, "INTERNAL_ERROR\n");
        }
    }
    
    if (!txt_format) {
        finalize_json(req, evb, &args, jsobj);
    }
    
    evhttp_send_reply(req, status_code, status_txt, evb);
    evhttp_clear_headers(&args);
}

void vanish_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct json_object  *jsobj;
    const char *json;
    
    tcadbvanish(adb);
    
    jsobj = json_object_new_object();
    json_object_object_add(jsobj, "status", json_object_new_string("ok"));
    json_object_object_add(jsobj, "value", json_object_new_int(1));
    
    json = json_object_to_json_string(jsobj);
    evbuffer_add_printf(evb, "%s\n", json);
    json_object_put(jsobj); // Odd free function
    
    evhttp_send_reply(req, HTTP_OK, "OK", evb);
}

void stats_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    int i;
    struct evkeyvalq args;
    const char *format;
    
    struct simplehttp_stats *st;
    
    st = simplehttp_stats_new();
    simplehttp_stats_get(st);
    
    evhttp_parse_query(req->uri, &args);
    format = (char *)evhttp_find_header(&args, "format");
    
    if ((format != NULL) && (strcmp(format, "json") == 0)) {
        evbuffer_add_printf(evb, "{");
        for (i = 0; i < st->callback_count; i++) {
            evbuffer_add_printf(evb, "\"%s_95\": %"PRIu64",", st->stats_labels[i], st->ninety_five_percents[i]);
            evbuffer_add_printf(evb, "\"%s_average_request\": %"PRIu64",", st->stats_labels[i], st->average_requests[i]);
            evbuffer_add_printf(evb, "\"%s_requests\": %"PRIu64",", st->stats_labels[i], st->stats_counts[i]);
        }
        evbuffer_add_printf(evb, "\"total_requests\": %"PRIu64, st->requests);
        evbuffer_add_printf(evb, "}\n");
    } else {
        evbuffer_add_printf(evb, "total requests: %"PRIu64"\n", st->requests);
        for (i = 0; i < st->callback_count; i++) {
            evbuffer_add_printf(evb, "/%s 95%%: %"PRIu64"\n", st->stats_labels[i], st->ninety_five_percents[i]);
            evbuffer_add_printf(evb, "/%s average request (usec): %"PRIu64"\n", st->stats_labels[i], st->average_requests[i]);
            evbuffer_add_printf(evb, "/%s requests: %"PRIu64"\n", st->stats_labels[i], st->stats_counts[i]);
        }
    }
    
    simplehttp_stats_free(st);
    
    evhttp_send_reply(req, HTTP_OK, "OK", evb);
    evhttp_clear_headers(&args);
}

void exit_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    fprintf(stdout, "/exit request recieved\n");
    event_loopbreak();
}

void info()
{
    fprintf(stdout, "%s: simplehttp in-memory tokyo cabinet abstract database.\n", NAME);
    fprintf(stdout, "Version %s, https://github.com/bitly/simplehttp/tree/master/simplememdb\n", VERSION);
}

void usage()
{
    fprintf(stderr, "%s: simplehttp in-memory tokyo cabinet abstract database.\n", NAME);
    fprintf(stderr, "\n");
    fprintf(stderr, "usage: %s\n", NAME);
    fprintf(stderr, "\t-a 127.0.0.1 (address to listen on)\n");
    fprintf(stderr, "\t-p 8080 (port to listen on)\n");
    fprintf(stderr, "\t-D (daemonize)\n");
    fprintf(stderr, "\n");
    exit(1);
}

void do_dump(int fd, short what, void *ctx)
{
    struct evhttp_request *req;
    struct evbuffer *evb;
    struct evkeyvalq args;
    int n, c = 0, set_timer = 0, send_reply = 0;
    int limit = 500; // dump 500 at a time
    char *key;
    void *value;
    const char *string;
    int string_mode = 0;
    int ovector[30], rc;
    
    req = (struct evhttp_request *)ctx;
    evb = req->output_buffer;
    
    evhttp_parse_query(req->uri, &args);
    string = (char *)evhttp_find_header(&args, "string");
    if (string) {
        string_mode = atoi(string);
    }
    evhttp_clear_headers(&args);
    
    while ((key = tcadbiternext2(adb)) != NULL) {
        if (dump_regex) {
            rc = pcre_exec(
                    dump_regex,           /* the compiled pattern */
                    NULL,                 /* no extra data - we didn't study the pattern */
                    key,                  /* the subject string */
                    strlen(key),          /* the length of the subject */
                    0,                    /* start at offset 0 in the subject */
                    0,                    /* default options */
                    ovector,              /* output vector for substring information */
                    sizeof(ovector));     /* number of elements in the output vector */
        }
        
        if ((dump_regex && (rc > 0)) || !dump_regex) {
            value = tcadbget(adb, key, strlen(key), &n);
            if (value) {
                if (string_mode) {
                    evbuffer_add_printf(evb, "%s,%s\n", key, (char *)value);
                } else {
                    evbuffer_add_printf(evb, "%s,%d\n", key, *(int *)value);
                }
                free(value);
            }
            free(key);
            send_reply = 1;
        }
        
        c++;
        if (c == limit) {
            set_timer = 1;
            break;
        }
    }
    
    if (send_reply) {
        evhttp_send_reply_chunk(req, evb);
    }
    
    if (set_timer) {
        set_dump_timer(req);
    } else {
        evhttp_send_reply_end(req);
        if (dump_regex) {
            pcre_free(dump_regex);
        }
        is_currently_dumping = 0;
    }
}

void set_dump_timer(struct evhttp_request *req)
{
    struct timeval tv = {0,500000}; // loop every 0.5 seconds
    
    evtimer_del(&ev);
    evtimer_set(&ev, do_dump, req);
    evtimer_add(&ev, &tv);
}

void dump_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct evkeyvalq args;
    int error_offset;
    const char *error;
    const char *regex;
    
    if (is_currently_dumping) {
        evbuffer_add_printf(evb, "ALREADY_DUMPING");
        evhttp_send_reply(req, 500, "ALREADY_DUMPING", evb);
        return;
    }
    
    is_currently_dumping = 1;
    
    evhttp_parse_query(req->uri, &args);
    regex = (char *)evhttp_find_header(&args, "regex");
    if (regex) {
        fprintf(stdout, "pcre_compile %s\n", regex);
        dump_regex = pcre_compile(regex, 0, &error, &error_offset, NULL);
        if (!dump_regex) {
            fprintf(stderr, "ERROR: pcre_compile - %s - offset %d\n", error, error_offset);
        }
    } else {
        dump_regex = NULL;
    }
    evhttp_clear_headers(&args);
    
    tcadbiterinit(adb);
    evhttp_send_reply_start(req, 200, "OK");
    set_dump_timer(req);
}

int main(int argc, char **argv)
{
    char buf[SM_BUFFER_SZ];
    unsigned long bnum = 1024;
    int ch;
    
    info();
    
    opterr=0;
    while ((ch = getopt(argc, argv, "b:vh")) != -1) {
        if (ch == '?') {
            optind--; // re-set for next getopt() parse by simplehttp_init
            break;
        }
        switch (ch) {
            case 'b':
                bnum = atoi(optarg);
                break;
            case 'v':
                exit(1);
                break;
            case 'h':
                usage();
                exit(1);
                break;
        }
    }
    
    sprintf(buf, "+#bnum=%lu", bnum);
    adb = tcadbnew();
    if (!tcadbopen(adb, buf)) {
        fprintf(stderr, "adb open error\n");
        exit(1);
    }
    
    simplehttp_init();
    simplehttp_set_cb("/get_int*", get_int_cb, NULL);
    simplehttp_set_cb("/get*", get_cb, NULL);
    simplehttp_set_cb("/mget_int*", mget_int_cb, NULL);
    simplehttp_set_cb("/mget*", mget_cb, NULL);
    simplehttp_set_cb("/put*", put_cb, NULL);
    simplehttp_set_cb("/del*", del_cb, NULL);
    simplehttp_set_cb("/vanish*", vanish_cb, NULL);
    simplehttp_set_cb("/fwmatch_int*", fwmatch_int_cb, NULL);
    simplehttp_set_cb("/fwmatch*", fwmatch_cb, NULL);
    simplehttp_set_cb("/incr*", incr_cb, NULL);
    simplehttp_set_cb("/dump*", dump_cb, NULL);
    simplehttp_set_cb("/stats*", stats_cb, NULL);
    simplehttp_set_cb("/exit", exit_cb, NULL);
    simplehttp_main(argc, argv);
    
    tcadbclose(adb);
    tcadbdel(adb);
    
    return 0;
}
