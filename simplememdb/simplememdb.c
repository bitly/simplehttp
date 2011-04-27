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
#define VERSION                 "1.4"
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

void finalize_request(struct evhttp_request *req, struct evbuffer *evb, struct evkeyvalq *args, struct json_object *jsobj)
{
    const char *json, *jsonp;
    if (jsobj) {
        jsonp = (char *)evhttp_find_header(args, "jsonp");
        json = (char *)json_object_to_json_string(jsobj);
        if (jsonp) {
            evbuffer_add_printf(evb, "%s(%s)\n", jsonp, json);
        } else {
            evbuffer_add_printf(evb, "%s\n", json);
        }
        json_object_put(jsobj); // Odd free function
    }
    // don't send the request if it was already sent
    if (!req->response_code) {
        evhttp_send_reply(req, HTTP_OK, "OK", evb);
    }
    evhttp_clear_headers(args);
}

void db_error_to_json(const char *msg, struct json_object *jsobj)
{
    json_object_object_add(jsobj, "status", json_object_new_string("error"));
    json_object_object_add(jsobj, "message", json_object_new_string(msg));
}

void db_error_to_txt(const char *msg, struct evbuffer *evb)
{
    if (EVBUFFER_LENGTH(evb)) {
        fprintf(stderr, "draining existing response\n");
        evbuffer_drain(evb, EVBUFFER_LENGTH(evb));
    }
    evbuffer_add_printf(evb, "INTERNAL_ERROR: %s", msg);
}

/* 
forward match for "key" casting values to int
?format=json returns {"results":[{k:v},{k,v}, ...]} 
?format=txt returns k,v\nk,v\n...
*/
void fwmatch_int_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char *key, *kbuf;
    int format;
    int *value;
    int i, max, off, len, n, list_count;
    TCLIST *keylist = NULL;
    struct evkeyvalq args;
    struct json_object *jsobj = NULL, *jsobj2, *jsarr = NULL;
    
    evhttp_parse_query(req->uri, &args);
    
    key = (char *)evhttp_find_header(&args, "key");
    if ((key == NULL) || (strlen(key) < 1)) {
        evhttp_send_error(req, 400, "INVALID_ARG_KEY");
        evhttp_clear_headers(&args);
        return;
    }
    
    format = get_argument_format(&args);
    max = get_int_argument(&args, "max", 1000);
    len = get_int_argument(&args, "length", 10);
    off = get_int_argument(&args, "offset", 0);
    
    if (format == json_format) {
        jsobj = json_object_new_object();
        jsarr = json_object_new_array();
    }
    
    keylist = tcadbfwmkeys(adb, key, strlen(key), max);
    if (keylist == NULL) {
        if (format == json_format) {
            db_error_to_json("INTERNAL_ERROR", jsobj);
        } else {
            db_error_to_txt("INTERNAL_ERROR\n", evb);
        }
    }
    
    list_count = tclistnum(keylist);
    for (i = off; keylist != NULL && i < (len + off) && i < list_count; i++) {
      kbuf = (char *)tclistval2(keylist, i);
      value = (int *)tcadbget(adb, kbuf, strlen(kbuf), &n);
      if (value) {
          if (format == json_format) {
              jsobj2 = json_object_new_object();
              json_object_object_add(jsobj2, kbuf, json_object_new_int((int)*value));
              json_object_array_add(jsarr, jsobj2);
          } else {
              evbuffer_add_printf(evb, "%s,%d\n", kbuf, (int)*value);
          }
          tcfree(value);
      }
    }
    tclistdel(keylist);
    
    if (format == txt_format) {
        evbuffer_add(evb, "\n", 1);
    }
    
    if (format == json_format) {
        json_object_object_add(jsobj, "results", jsarr);
        json_object_object_add(jsobj, "status", json_object_new_string(list_count ? "ok" : "no results"));
    }
    
    finalize_request(req, evb, &args, jsobj);
}

/* same operation as fwmatch_int but merges keys based on the prefix

GET /fwmatch_int_merged?key=prefix.
data:
    prefix.a,1
    prefix.b,1

returns:
prefix,a:1 b:1

Note: for convenience the last character of key is dropped (it is assumed that it's a deliminator)
*/
void fwmatch_int_merged_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key, *kbuf;
    int                 *value;
    int                 i, max, off, len, list_count, n;
    int                 started_output = 0;
    int                 format;
    TCLIST              *keylist = NULL;
    struct evkeyvalq    args;
    struct json_object  *jsobj = NULL, *jsobj2, *jsarr = NULL;
    
    evhttp_parse_query(req->uri, &args);
    
    key = (char *)evhttp_find_header(&args, "key");
    if ((key == NULL) || (strlen(key) < 1)) {
        evhttp_send_error(req, 400, "INVALID_ARG_KEY");
        evhttp_clear_headers(&args);
        return;
    }
    
    format = get_argument_format(&args);
    max = get_int_argument(&args, "max", 1000);
    len = get_int_argument(&args, "length", 10);
    off = get_int_argument(&args, "offset", 0);
    
    if (format == json_format) {
        jsobj = json_object_new_object();
        jsarr = json_object_new_array();
    }
    
    keylist = tcadbfwmkeys(adb, key, strlen(key), max);
    if (keylist == NULL) {
        if (format == json_format) {
            db_error_to_json("INTERNAL_ERROR", jsobj);
        } else {
            db_error_to_txt("INTERNAL_ERROR\n", evb);
        }
    }
    
    list_count = tclistnum(keylist);
    for (i = off; keylist != NULL && i < (len + off) && i < list_count; i++) {
        kbuf = (char *)tclistval2(keylist, i);
        value = (int *)tcadbget(adb, kbuf, strlen(kbuf), &n);
        if (value) {
            if (format == txt_format){
                if (!started_output) {
                    evbuffer_add(evb, key, strlen(key) - 1);
                    evbuffer_add(evb, ",", 1);
                }
                if (started_output) {
                    evbuffer_add(evb, " ", 1);
                }
                started_output = 1;
                // write the trailing part of this key
                evbuffer_add(evb, kbuf + strlen(key), strlen(kbuf) - strlen(key));
                // write the : + value
                evbuffer_add_printf(evb, ":%d", (int)*value);
            } else if (format == json_format) {
                jsobj2 = json_object_new_object();
                json_object_object_add(jsobj2, kbuf, json_object_new_int((int) *value));
                json_object_array_add(jsarr, jsobj2);
            }
            tcfree(value);
        }
    }
    tclistdel(keylist);
    
    if (format == txt_format) {
        evbuffer_add(evb, "\n", 1);
    }
    
    if (format == json_format) {
        json_object_object_add(jsobj, "results", jsarr);
        json_object_object_add(jsobj, "status", json_object_new_string(list_count ? "ok" : "no results"));
    }
    
    finalize_request(req, evb, &args, jsobj);
}

void fwmatch_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char *key, *kbuf, *value;
    int format;
    int i, max, off, len, n, list_count;
    TCLIST *keylist = NULL;
    struct evkeyvalq args;
    struct json_object *jsobj = NULL, *jsobj2, *jsarr = NULL;
    
    evhttp_parse_query(req->uri, &args);
    
    key = (char *)evhttp_find_header(&args, "key");
    if ((key == NULL) || (strlen(key) < 1)) {
        evhttp_send_error(req, 400, "INVALID_ARG_KEY");
        evhttp_clear_headers(&args);
        return;
    }
    
    format = get_argument_format(&args);
    max = get_int_argument(&args, "max", 1000);
    len = get_int_argument(&args, "length", 10);
    off = get_int_argument(&args, "offset", 0);
    
    if (format == json_format) {
        jsobj = json_object_new_object();
        jsarr = json_object_new_array();
    }
    
    keylist = tcadbfwmkeys(adb, key, strlen(key), max);
    if (keylist == NULL) {
        if (format == json_format) {
            db_error_to_json("INTERNAL_ERROR", jsobj);
        } else {
            db_error_to_txt("INTERNAL_ERROR\n", evb);
        }
    }
    
    list_count = tclistnum(keylist);
    for (i = off; keylist != NULL && i < (len+off) && i < list_count; i++) {
        kbuf = (char *)tclistval2(keylist, i);
        value = tcadbget(adb, kbuf, strlen(kbuf), &n);
        if (value) {
            if (format == json_format) {
                jsobj2 = json_object_new_object();
                json_object_object_add(jsobj2, kbuf, json_object_new_string(value));
                json_object_array_add(jsarr, jsobj2);
            } else {
                evbuffer_add_printf(evb, "%s,%s\n", kbuf, value);
            }
            tcfree(value);
        }
    }
    tclistdel(keylist);
    
    if (format == txt_format) {
        evbuffer_add(evb, "\n", 1);
    }
    
    if (format == json_format) {
        json_object_object_add(jsobj, "results", jsarr);
        json_object_object_add(jsobj, "status", json_object_new_string(list_count ? "ok" : "no results"));
    }
    
    finalize_request(req, evb, &args, jsobj);
}

void del_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key;
    int                 format;
    struct evkeyvalq    args;
    struct json_object  *jsobj = NULL;
    
    evhttp_parse_query(req->uri, &args);
    
    key = (char *)evhttp_find_header(&args, "key");
    if (key == NULL) {
        evhttp_send_error(req, 400, "INVALID_ARG_KEY");
        evhttp_clear_headers(&args);
        return;
    }
    
    format = get_argument_format(&args);
    if (format == json_format) {
        jsobj = json_object_new_object();
    }
    
    if (tcadbout(adb, key, strlen(key))) {
        if (format == json_format) {
            json_object_object_add(jsobj, "status", json_object_new_string("ok"));
        } else {
            evbuffer_add_printf(evb, "OK\n");
        }
    } else {
        if (format == json_format) {
            db_error_to_json("INTERNAL_ERROR", jsobj);
        } else {
            db_error_to_txt("INTERNAL_ERROR\n", evb);
        }
    }
    
    finalize_request(req, evb, &args, jsobj);
}

void put_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key, *value;
    int                 format;
    struct evkeyvalq    args;
    struct json_object  *jsobj = NULL;
    
    evhttp_parse_query(req->uri, &args);
    
    key = (char *)evhttp_find_header(&args, "key");
    if (key == NULL) {
        evhttp_send_error(req, 400, "INVALID_ARG_KEY");
        evhttp_clear_headers(&args);
        return;
    }
    
    value = (char *)evhttp_find_header(&args, "value");
    if (value == NULL) {
        evhttp_send_error(req, 400, "INVALID_ARG_VALUE");
        evhttp_clear_headers(&args);
        return;
    }
    
    format = get_argument_format(&args);
    if (format == json_format) {
        jsobj = json_object_new_object();
    }
    
    jsobj = json_object_new_object();
    if (tcadbput(adb, key, strlen(key), value, strlen(value))) {
        if (format == json_format) {
            json_object_object_add(jsobj, "status", json_object_new_string("ok"));
        } else {
            evbuffer_add_printf(evb, "OK\n");
        }
    } else {
        if (format == json_format) {
            db_error_to_json("INTERNAL_ERROR", jsobj);
        } else {
            db_error_to_txt("INTERNAL_ERROR\n", evb);
        }
    }
    
    finalize_request(req, evb, &args, jsobj);
}

void get_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char *key, *value;
    struct evkeyvalq args;
    struct json_object *jsobj = NULL;
    int n;
    int format;
    
    evhttp_parse_query(req->uri, &args);
    
    key = (char *)evhttp_find_header(&args, "key");
    if (key == NULL) {
        evhttp_send_error(req, 400, "INVALID_ARG_KEY");
        evhttp_clear_headers(&args);
        return;
    }
    
    format = get_argument_format(&args);
    if (format == json_format) {
        jsobj = json_object_new_object();
    }
    
    if ((value = tcadbget(adb, key, strlen(key), &n))) {
        if (format == json_format) {
            json_object_object_add(jsobj, "status", json_object_new_string("ok"));
            json_object_object_add(jsobj, "value", json_object_new_string(value));
        } else {
            evbuffer_add_printf(evb, "%s\n", value);
        }
        free(value);
    } else {
        if (format == json_format) {
            db_error_to_json("INTERNAL_ERROR", jsobj);
        } else {
            db_error_to_txt("INTERNAL_ERROR\n", evb);
        }
    }
    
    finalize_request(req, evb, &args, jsobj);
}

void get_int_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char *key;
    int *value;
    int format;
    struct evkeyvalq args;
    struct json_object *jsobj = NULL;
    int n;
    
    evhttp_parse_query(req->uri, &args);
    
    key = (char *)evhttp_find_header(&args, "key");
    if (key == NULL) {
        evhttp_send_error(req, 400, "INVALID_ARG_KEY");
        evhttp_clear_headers(&args);
        return;
    }
    
    format = get_argument_format(&args);
    if (format == json_format) {
        jsobj = json_object_new_object();
    }
    
    if ((value = (int *)tcadbget(adb, key, strlen(key), &n))) {
        if (format == json_format) {
            json_object_object_add(jsobj, "status", json_object_new_string("ok"));
            json_object_object_add(jsobj, "value", json_object_new_int((int)*value));
        } else {
            evbuffer_add_printf(evb, "%d\n", (int)*value);
        }
        free(value);
    } else {
        if (format == json_format) {
            db_error_to_json("INTERNAL_ERROR", jsobj);
        } else {
            db_error_to_txt("INTERNAL_ERROR\n", evb);
        }
    }
    
    finalize_request(req, evb, &args, jsobj);
}

void mget_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char *key, *value;
    struct evkeyvalq args;
    struct evkeyval *pair;
    struct json_object *jsobj = NULL;
    int nkeys = 0;
    int n;
    int format;
    
    evhttp_parse_query(req->uri, &args);
    
    format = get_argument_format(&args);
    if (format == json_format) {
        jsobj = json_object_new_object();
    }
    
    TAILQ_FOREACH(pair, &args, next) {
        if (pair->key[0] != 'k') continue;
        key = (char *)pair->value;
        nkeys++;
        
        value = tcadbget(adb, key, strlen(key), &n);
        if (value) {
            if (format == json_format) {
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
        if (format == json_format) {
            json_object_put(jsobj);
        }
        return;
    }
    
    finalize_request(req, evb, &args, jsobj);
}

void mget_int_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char *key;
    int *value;
    struct evkeyvalq args;
    struct evkeyval *pair;
    struct json_object *jsobj = NULL;
    int nkeys = 0;
    int n;
    int format;
    
    evhttp_parse_query(req->uri, &args);
    
    format = get_argument_format(&args);
    if (format == json_format) {
        jsobj = json_object_new_object();
    }
    
    TAILQ_FOREACH(pair, &args, next) {
        if (pair->key[0] != 'k') continue;
        key = (char *)pair->value;
        nkeys++;
        
        value = (int *)tcadbget(adb, key, strlen(key), &n);
        if (value) {
            if (format == json_format) {
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
        if (format == json_format) {
            json_object_put(jsobj);
        }
        return;
    }
    
    finalize_request(req, evb, &args, jsobj);
}

void incr_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    int format;
    struct json_object *jsobj = NULL;
    struct evkeyvalq args;
    int v;
    int incr_value;
    struct evkeyval *arg;
    bool has_key_arg = false;
    bool error = false;
    
    evhttp_parse_query(req->uri, &args);
    
    incr_value = get_int_argument(&args, "value", 1);
    format = get_argument_format(&args);
    if (format == json_format) {
        jsobj = json_object_new_object();
    }
    
    TAILQ_FOREACH(arg, &args, next) {
        if (strcasecmp(arg->key, "key") == 0) {
            has_key_arg = true;
            if ((v = tcadbaddint(adb, arg->value, strlen(arg->value), incr_value)) == INT_MIN) {
                error = true;
                break;
            }
        }
    }
    
    if (!has_key_arg) {
        evhttp_send_error(req, 400, "INVALID_ARG_KEY");
        evhttp_clear_headers(&args);
        if (format == json_format) {
            json_object_put(jsobj);
        }
        return;
    }
    
    if (!error) {
        if (format == json_format) {
            json_object_object_add(jsobj, "status", json_object_new_string("ok"));
        } else {
            evbuffer_add_printf(evb, "OK\n");
        }
    } else {
        if (format == json_format) {
            db_error_to_json("INTERNAL_ERROR", jsobj);
        } else {
            db_error_to_txt("INTERNAL_ERROR\n", evb);
        }
    }
    
    finalize_request(req, evb, &args, jsobj);
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
    int format;
    
    struct simplehttp_stats *st;
    
    st = simplehttp_stats_new();
    simplehttp_stats_get(st);
    
    evhttp_parse_query(req->uri, &args);
    format = get_argument_format(&args);
    
    if (format == json_format) {
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
            send_reply = 1;
        }
        
        free(key);
        
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

/**
 * initiate a non-blocking asynchronous dump of key,value pairs
 * this is accomplished by writing x # of rows (default 500) to the output buffer via evtimers
 * giving libevent opportunity to handle other requests
 */
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
