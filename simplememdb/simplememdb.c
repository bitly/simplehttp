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
#include "lib/timer.h"
#include "lib/util.h"
#include "pcre.h"

#define NAME                    "simplememdb"
#define VERSION                 "1.2"
#define BUFFER_SZ               1048576
#define SM_BUFFER_SZ            4096

#define NUM_REQUEST_TYPES       10
#define NUM_REQUESTS_FOR_STATS  1000
#define STATS_GET               0
#define STATS_GET_INT           1
#define STATS_PUT               2
#define STATS_INCR              3
#define STATS_DEL               4
#define STATS_FWMATCH           5
#define STATS_FWMATCH_INT       6
#define STATS_VANISH            7
#define STATS_MGET              8
#define STATS_MGET_INT          9

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

static uint64_t requests = 0;
static uint64_t stats_request_counts[NUM_REQUEST_TYPES];
static int64_t stats_request[NUM_REQUESTS_FOR_STATS * NUM_REQUEST_TYPES];
static int stats_request_idx[NUM_REQUEST_TYPES];
static char *stats_request_labels[] = { "get", "get_int", "put", "incr", "del", "fwmatch", "fwmatch_int", "vanish", "mget", "mget_int" };

static TCADB *adb;
static int is_currently_dumping = 0;
static struct event ev;
static pcre *dump_regex;

void stats_store_request(int index, unsigned int diff)
{
    stats_request[(index * NUM_REQUESTS_FOR_STATS) + stats_request_idx[index]] = diff;
    stats_request_idx[index]++;
    
    if (stats_request_idx[index] >= NUM_REQUESTS_FOR_STATS) {
        stats_request_idx[index] = 0;
    }
}

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

    evhttp_send_reply(req, HTTP_OK, "OK", evb);
    evhttp_clear_headers(args);
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
    char                *key, *kbuf, *format;
    int                 *value;
    int                 i, max, off, len;
    TCLIST              *keylist = NULL;
    struct evkeyvalq    args;
    struct json_object  *jsobj = NULL, *jsobj2, *jsarr = NULL;
    int txt_format = 0;
    
    _gettime(&ts1);
    
    requests++;
    stats_request_counts[STATS_FWMATCH_INT]++;
    
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
    
    if (!strncmp(format, "txt", 3)) {
        txt_format = 1;
    }
    
    if (!txt_format) {
        jsobj = json_object_new_object();
        jsarr = json_object_new_array();
    }
    
    keylist = tcadbfwmkeys(adb, key, strlen(key), max);
    for (i=off; keylist!=NULL && i<(len+off) && i<tclistnum(keylist); i++){
      kbuf = (char *)tclistval2(keylist, i);
      value = (int *)tcadbget2(adb, kbuf);
      if (value) {
          if (!txt_format) {
              jsobj2 = json_object_new_object();
              json_object_object_add(jsobj2, kbuf, json_object_new_int((int)*value));
              json_object_array_add(jsarr, jsobj2);
          } else {
              evbuffer_add_printf(evb, "%d\n", (int)*value);
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
        if (!txt_format) {
            db_error_to_json("INTERNAL_ERROR", jsobj);
        } else {
            evbuffer_add_printf(evb, "INTERNAL_ERROR\n");
        }
    }
    
    if (!txt_format) {
        finalize_json(req, evb, &args, jsobj);
    }
    
    _gettime(&ts2);
    stats_store_request(STATS_FWMATCH_INT, _ts_diff(ts1, ts2));
}

void fwmatch_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key, *kbuf, *value, *format;
    int                 i, max, off, len, n;
    TCLIST              *keylist = NULL;
    struct evkeyvalq    args;
    struct json_object  *jsobj, *jsobj2, *jsarr;
    int txt_format = 0;
    
    _gettime(&ts1);
    
    requests++;
    stats_request_counts[STATS_FWMATCH]++;
    
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
    
    if (!strncmp(format, "txt", 3)) {
        txt_format = 1;
    }
    
    if (!txt_format) {
        jsobj = json_object_new_object();
        jsarr = json_object_new_array();
    }
    
    keylist = tcadbfwmkeys(adb, key, strlen(key), max);
    for (i=off; keylist!=NULL && i<(len+off) && i<tclistnum(keylist); i++) {
        kbuf = (char *)tclistval2(keylist, i);
        value = (int *)tcadbget2(adb, kbuf);
        if (value) {
            if (!txt_format) {
                jsobj2 = json_object_new_object();
                json_object_object_add(jsobj2, kbuf, json_object_new_string(value));
                json_object_array_add(jsarr, jsobj2);
            } else {
                evbuffer_add_printf(evb, "%d\n", value);
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
        if (!txt_format) {
            db_error_to_json("INTERNAL_ERROR", jsobj);
        } else {
            evbuffer_add_printf(evb, "INTERNAL_ERROR\n");
        }
    }
    
    if (!txt_format) {
        finalize_json(req, evb, &args, jsobj);
    }
    
    _gettime(&ts2);
    stats_store_request(STATS_FWMATCH, _ts_diff(ts1, ts2));
}

void del_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key, *format;
    struct evkeyvalq    args;
    struct json_object  *jsobj;
    int txt_format = 0;
    
    _gettime(&ts1);
    
    requests++;
    stats_request_counts[STATS_DEL]++;
    
    evhttp_parse_query(req->uri, &args);
    key = (char *)evhttp_find_header(&args, "key");
    format = (char *)evhttp_find_header(&args, "format");
    if (key == NULL) {
        evhttp_send_error(req, 400, "INVALID_ARG_KEY");
        evhttp_clear_headers(&args);
        return;
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
        if (!txt_format) {
            db_error_to_json("INTERNAL_ERROR", jsobj);
        } else {
            evbuffer_add_printf(evb, "INTERNAL_ERROR\n");
        }
    }
    
    if (!txt_format) {
        finalize_json(req, evb, &args, jsobj);
    }
    
    _gettime(&ts2);
    stats_store_request(STATS_DEL, _ts_diff(ts1, ts2));
}

void put_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key, *value;
    struct evkeyvalq    args;
    struct json_object  *jsobj;
    
    _gettime(&ts1);
    
    requests++;
    stats_request_counts[STATS_PUT]++;
    
    if (adb == NULL) {
        evhttp_send_error(req, 503, "database not connected");
        return;
    }
    evhttp_parse_query(req->uri, &args);
    
    key = (char *)evhttp_find_header(&args, "key");
    value = (char *)evhttp_find_header(&args, "value");
    if (key == NULL) {
        evhttp_send_error(req, 400, "key is required");
        evhttp_clear_headers(&args);
        return;
    }
    if (value == NULL) {
        evhttp_send_error(req, 400, "value is required");
        evhttp_clear_headers(&args);
        return;
    }
    
    jsobj = json_object_new_object();
    if (tcadbput(adb, key, strlen(key), value, strlen(value))) {
        json_object_object_add(jsobj, "status", json_object_new_string("ok"));
        json_object_object_add(jsobj, "value", json_object_new_string(value));
    } else {
        db_error_to_json("tcadbput failed", jsobj);
    }
    
    finalize_json(req, evb, &args, jsobj);
    
    _gettime(&ts2);
    stats_store_request(STATS_PUT, _ts_diff(ts1, ts2));
}

void get_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key, *value;
    struct evkeyvalq    args;
    struct json_object  *jsobj;
    int n;
    
    _gettime(&ts1);
    
    requests++;
    stats_request_counts[STATS_GET]++;
    
    if (adb == NULL) {
        evhttp_send_error(req, 503, "database not connected");
        return;
    }
    
    evhttp_parse_query(req->uri, &args);
    
    key = (char *)evhttp_find_header(&args, "key");
    if (key == NULL) {
        evhttp_send_error(req, 400, "key is required");
        evhttp_clear_headers(&args);
        return;
    }
    
    jsobj = json_object_new_object();
    value = tcadbget(adb, key, strlen(key), &n);
    if (value) {
        json_object_object_add(jsobj, "status", json_object_new_string("ok"));
        json_object_object_add(jsobj, "value", json_object_new_string(value));
        free(value);
    } else {
        db_error_to_json("tcadbget2 failed", jsobj);
    }
    
    finalize_json(req, evb, &args, jsobj);
    
    _gettime(&ts2);
    stats_store_request(STATS_GET, _ts_diff(ts1, ts2));
}

void get_int_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key;
    int                 *value;
    struct evkeyvalq    args;
    struct json_object  *jsobj;
    int n;
    
    _gettime(&ts1);
    
    requests++;
    stats_request_counts[STATS_GET_INT]++;
    
    if (adb == NULL) {
        evhttp_send_error(req, 503, "database not connected");
        return;
    }
    
    evhttp_parse_query(req->uri, &args);
    
    key = (char *)evhttp_find_header(&args, "key");
    if (key == NULL) {
        evhttp_send_error(req, 400, "key is required");
        evhttp_clear_headers(&args);
        return;
    }
    
    jsobj = json_object_new_object();
    value = (int *)tcadbget(adb, key, strlen(key), &n);
    if (value) {
        json_object_object_add(jsobj, "status", json_object_new_string("ok"));
        json_object_object_add(jsobj, "value", json_object_new_int((int) *value));
        free(value);
    } else {
        db_error_to_json("tcadbget failed", jsobj);
    }
    
    finalize_json(req, evb, &args, jsobj);
    
    _gettime(&ts2);
    stats_store_request(STATS_GET_INT, _ts_diff(ts1, ts2));
}

void mget_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key, *value;
    struct evkeyvalq    args;
    struct evkeyval *pair;
    struct json_object  *jsobj, *jserr;
    int nkeys = 0;
    int n;
    
    _gettime(&ts1);
    
    requests++;
    stats_request_counts[STATS_MGET]++;
    
    if (adb == NULL) {
        evhttp_send_error(req, 503, "database not connected");
        return;
    }
    
    jsobj = json_object_new_object();
    
    evhttp_parse_query(req->uri, &args);
    TAILQ_FOREACH(pair, &args, next) {
        if (pair->key[0] != 'k') continue;
        key = (char *)pair->value;
        nkeys++;
        
        value = tcadbget(adb, key, strlen(key), &n);
        if (value) {
            json_object_object_add(jsobj, key, json_object_new_string(value));
            free(value);
        } else {
            jserr = json_object_new_object();
            db_error_to_json("tcadbget failed", jserr);
            json_object_object_add(jsobj, key, jserr);
        }
    }
    
    if (!nkeys) {
        evhttp_send_error(req, 400, "key is required");
        evhttp_clear_headers(&args);
        return;
    }
    
    finalize_json(req, evb, &args, jsobj);
    
    _gettime(&ts2);
    stats_store_request(STATS_MGET, _ts_diff(ts1, ts2));
}

void mget_int_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key;
    int                 *value;
    struct evkeyvalq    args;
    struct evkeyval *pair;
    struct json_object  *jsobj, *jserr;
    int nkeys = 0;
    int n;
    
    _gettime(&ts1);
    
    requests++;
    stats_request_counts[STATS_MGET_INT]++;
    
    if (adb == NULL) {
        evhttp_send_error(req, 503, "database not connected");
        return;
    }
    
    jsobj = json_object_new_object();
    
    evhttp_parse_query(req->uri, &args);
    TAILQ_FOREACH(pair, &args, next) {
        if (pair->key[0] != 'k') continue;
        key = (char *)pair->value;
        nkeys++;
        
        value = (int *)tcadbget(adb, key, strlen(key), &n);
        if (value) {
            json_object_object_add(jsobj, key, json_object_new_int((int)*value));
            free(value);
        } else {
            jserr = json_object_new_object();
            db_error_to_json("tcadbget failed", jserr);
            json_object_object_add(jsobj, key, jserr);
        }
    }
    
    if (!nkeys) {
        evhttp_send_error(req, 400, "key is required");
        evhttp_clear_headers(&args);
        return;
    }
    
    finalize_json(req, evb, &args, jsobj);
    
    _gettime(&ts2);
    stats_store_request(STATS_MGET_INT, _ts_diff(ts1, ts2));
}

void incr_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char *incr_value;
    struct json_object  *jsobj;
    struct evkeyvalq args;
    int v;
    int value = 1;
    struct evkeyval *arg;
    bool has_key_arg = false;
    bool error = false;
    
    _gettime(&ts1);
    
    requests++;
    stats_request_counts[STATS_INCR]++;
    
    if (adb == NULL) {
        evhttp_send_error(req, 503, "no database");
        return;
    }
    
    evhttp_parse_query(req->uri, &args);
    
    incr_value = (char *)evhttp_find_header(&args, "value");
    if (incr_value != NULL) {
        value = atoi(incr_value);
    }
    
    jsobj = json_object_new_object();
    TAILQ_FOREACH(arg, &args, next) {
        if (strcasecmp(arg->key, "key") == 0) {
            has_key_arg = true;
            if ((v = tcadbaddint(adb, arg->value, strlen(arg->value), value)) == INT_MIN) {
                error = true;
                db_error_to_json("tcadbaddint failed", jsobj);
                break;
            }
        }
    }
    
    if (!has_key_arg) {
        evhttp_send_error(req, 400, "key is required");
        evhttp_clear_headers(&args);
        json_object_put(jsobj);
        return;
    }
    
    if (!error) json_object_object_add(jsobj, "status", json_object_new_string("ok"));
    
    finalize_json(req, evb, &args, jsobj);
    
    _gettime(&ts2);
    stats_store_request(STATS_INCR, _ts_diff(ts1, ts2));
}

void vanish_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct json_object  *jsobj;
    const char *json;
    
    requests++;
    
    if (adb == NULL) {
        evhttp_send_error(req, 503, "no database");
        return;
    }
    
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
    uint64_t request_total;
    uint64_t average_requests[NUM_REQUEST_TYPES];
    uint64_t ninety_five_percents[NUM_REQUEST_TYPES];
    int i, j, c, request_array_end;
    struct evkeyvalq args;
    const char *format;
    
    memset(&average_requests, 0, sizeof(average_requests));
    memset(&ninety_five_percents, 0, sizeof(ninety_five_percents));
    
    for (i = 0; i < NUM_REQUEST_TYPES; i++) {
        request_total = 0;
        for (j = (i * NUM_REQUESTS_FOR_STATS), request_array_end = j + NUM_REQUESTS_FOR_STATS, c = 0; 
            (j < request_array_end) && (stats_request[j] != -1); j++, c++) {
            request_total += stats_request[j];
        }
        if (c) {
            average_requests[i] = request_total / c;
            ninety_five_percents[i] = ninety_five_percent(stats_request + (i * NUM_REQUESTS_FOR_STATS), c);
        }
    }
    
    evhttp_parse_query(req->uri, &args);
    format = (char *)evhttp_find_header(&args, "format");
    
    if ((format != NULL) && (strcmp(format, "json") == 0)) {
        evbuffer_add_printf(evb, "{");
        for (i = 0; i < NUM_REQUEST_TYPES; i++) {
            evbuffer_add_printf(evb, "\"%s_95\": %"PRIu64",", stats_request_labels[i], ninety_five_percents[i]);
            evbuffer_add_printf(evb, "\"%s_average_request\": %"PRIu64",", stats_request_labels[i], average_requests[i]);
            evbuffer_add_printf(evb, "\"%s_requests\": %"PRIu64",", stats_request_labels[i], stats_request_counts[i]);
        }
        evbuffer_add_printf(evb, "\"total_requests\": %"PRIu64, requests);
        evbuffer_add_printf(evb, "}\n");
    } else {
        evbuffer_add_printf(evb, "total requests: %"PRIu64"\n", requests);
        for (i = 0; i < NUM_REQUEST_TYPES; i++) {
            evbuffer_add_printf(evb, "/%s 95%%: %"PRIu64"\n", stats_request_labels[i], ninety_five_percents[i]);
            evbuffer_add_printf(evb, "/%s average request (usec): %"PRIu64"\n", stats_request_labels[i], average_requests[i]);
            evbuffer_add_printf(evb, "/%s requests: %"PRIu64"\n", stats_request_labels[i], stats_request_counts[i]);
        }
    }
    
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
    fprintf(stderr, "%s: simplehttp in-memory tokyo cabinet abstract database.n", NAME);
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
    
    memset(&stats_request, -1, sizeof(stats_request));
    memset(&stats_request_idx, 0, sizeof(stats_request_idx));
    memset(&stats_request_counts, 0, sizeof(stats_request_counts));
    
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
