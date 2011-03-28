#include <tcrdb.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <inttypes.h>
#include <simplehttp/queue.h>
#include <simplehttp/simplehttp.h>
#include <json/json.h>

#define NAME                    "simpletokyo"
#define VERSION                 "1.7"
#define RECONNECT               5

void finalize_request(struct evhttp_request *req, struct evbuffer *evb, struct evkeyvalq *args, struct json_object *jsobj);
int open_db(char *addr, int port, TCRDB **rdb);
void close_db(TCRDB **rdb);
void db_reconnect(int fd, short what, void *ctx);
void db_error_to_json(int code, struct json_object *jsobj);
void db_error_to_txt(int code, struct evbuffer *evb);
void fwmatch_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void fwmatch_int_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void fwmatch_int_merged_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void del_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void put_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void get_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void get_int_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void mget_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void mget_int_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void incr_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void vanish_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void stats_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void exit_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);

struct event ev;
struct timeval tv = {RECONNECT,0};
static char *db_host = "127.0.0.1";
static int db_port = 1978;
static TCRDB *rdb;
static int db_status;
static uint64_t db_opened = 0;

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

void close_db(TCRDB **rdb)
{
    int ecode=0;
    if (*rdb != NULL) {
        if(!tcrdbclose(*rdb)){
          ecode = tcrdbecode(*rdb);
          fprintf(stderr, "close error: %s\n", tcrdberrmsg(ecode));
        }
        tcrdbdel(*rdb);
    }
}

int open_db(char *addr, int port, TCRDB **rdb)
{
    db_opened++;
    int ecode=0;
    
    if (*rdb != NULL) {
        fprintf(stderr, "closing db\n");
        if(!tcrdbclose(*rdb)){
          ecode = tcrdbecode(*rdb);
          fprintf(stderr, "close error: %s\n", tcrdberrmsg(ecode));
        }
        tcrdbdel(*rdb);
        *rdb = NULL;
    }
    fprintf(stderr, "opening db %s:%d\n", addr, port);
    *rdb = tcrdbnew();
    if(!tcrdbopen(*rdb, addr, port)){
        ecode = tcrdbecode(*rdb);
        fprintf(stderr, "ERROR: %s:%d %s\n", addr, port, tcrdberrmsg(ecode));
        *rdb = NULL;
    } else {
        char *status = tcrdbstat(*rdb);
        fprintf(stderr, "%s---------------------\n", status);
        if (status) free(status);
    }
    if (*rdb == NULL){
        fprintf(stderr, "db connection not open\n");
    }
    return ecode;
}

void db_reconnect(int fd, short what, void *ctx)
{
    int s;
    s = db_status;
    if (s != TTESUCCESS && s != TTEINVALID && s != TTEKEEP && s != TTENOREC) {
        db_status = open_db(db_host, db_port, &rdb);
    }
    evtimer_del(&ev);
    evtimer_set(&ev, db_reconnect, NULL);
    evtimer_add(&ev, &tv);
}

void db_error_to_json(int code, struct json_object *jsobj)
{
    // 7 is the error code for not found - we dont need to log this
    if (code != 7) {
        fprintf(stderr, "error(%d): %s\n", code, tcrdberrmsg(code));
    }
    json_object_object_add(jsobj, "status", json_object_new_string("error"));
    json_object_object_add(jsobj, "code", json_object_new_int(code));
    json_object_object_add(jsobj, "message", json_object_new_string((char *)tcrdberrmsg(code)));
}

void db_error_to_txt(int code, struct evbuffer *evb)
{
    // 7 is the error code for not found - we dont need to log this
    if (code != 7) {
        fprintf(stderr, "error(%d): %s\n", code, tcrdberrmsg(code));
    }
    if (EVBUFFER_LENGTH(evb)){
        fprintf(stderr, "draining existing response\n");
        evbuffer_drain(evb, EVBUFFER_LENGTH(evb));
    }
    evbuffer_add_printf(evb, "DB_ERROR: %s", (char *)tcrdberrmsg(code));
}

/* 
forward match for "key" casting values to int
?format=json returns {"results":[{k:v},{k,v}, ...]} 
?format=txt returns k,v\nk,v\n...
*/
void fwmatch_int_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key, *kbuf;
    int                 *value;
    int                 i, max, off, len, list_count;
    int                 format;
    TCLIST              *keylist = NULL;
    struct evkeyvalq    args;
    struct json_object  *jsobj, *jsobj2, *jsarr;
    jsobj = NULL;
    jsarr = NULL;
    
    if (rdb == NULL) {
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
    
    format = get_argument_format(&args);
    max = get_int_argument(&args, "max", 1000);
    len = get_int_argument(&args, "length", 10);
    off = get_int_argument(&args, "offset", 0);
    
    if (format == json_format) {
        jsobj = json_object_new_object();
        jsarr = json_object_new_array();
    }
    
    keylist = tcrdbfwmkeys(rdb, key, strlen(key), max);
    if (keylist == NULL) {
        db_status = tcrdbecode(rdb);
        if (format == txt_format) {
            db_error_to_txt(db_status, evb);
        } else {
            db_error_to_json(db_status, jsobj);
        }
    }
    
    list_count = tclistnum(keylist);
    for (i = off; keylist != NULL && i < (len+off) && i < list_count; i++) {
        kbuf = (char *)tclistval2(keylist, i);
        value = (int *)tcrdbget2(rdb, kbuf);
        if (value) {
            if (format == txt_format){
                evbuffer_add_printf(evb, "%s,%d\n", kbuf, (int)*value);
            } else {
                jsobj2 = json_object_new_object();
                json_object_object_add(jsobj2, kbuf, json_object_new_int((int) *value));
                json_object_array_add(jsarr, jsobj2);
            }
            tcfree(value);
        }
    }
    tclistdel(keylist);
    
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
    int                 i, max, off, len, list_count;
    int                 started_output = 0;
    int                 format;
    TCLIST              *keylist = NULL;
    struct evkeyvalq    args;
    struct json_object  *jsobj, *jsobj2, *jsarr;
    jsobj = NULL;
    jsarr = NULL;
    
    if (rdb == NULL) {
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
    if (strlen(key) < 1) {
        evhttp_send_error(req, 400, "key must be more than 1 character");
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
    
    keylist = tcrdbfwmkeys(rdb, key, strlen(key), max);
    if (keylist == NULL) {
        db_status = tcrdbecode(rdb);
        if (format == txt_format) {
            db_error_to_txt(db_status, evb);
        } else {
            db_error_to_json(db_status, jsobj);
        }
    }
    
    list_count = tclistnum(keylist);
    for (i = off; keylist != NULL && i < (len+off) && i < list_count; i++) {
        kbuf = (char *)tclistval2(keylist, i);
        value = (int *)tcrdbget2(rdb, kbuf);
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
    char                *key, *kbuf, *value;
    int                 i, max, off, len, list_count;
    int                 format;
    TCLIST              *keylist = NULL;
    struct evkeyvalq    args;
    struct json_object  *jsobj, *jsobj2, *jsarr;
    jsobj = NULL;
    jsarr = NULL;
    
    if (rdb == NULL) {
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
    
    format = get_argument_format(&args);
    max = get_int_argument(&args, "max", 1000);
    len = get_int_argument(&args, "length", 10);
    off = get_int_argument(&args, "offset", 0);
    
    if (format == json_format) {
        jsobj = json_object_new_object();
        jsarr = json_object_new_array();
    }
    
    keylist = tcrdbfwmkeys(rdb, key, strlen(key), max);
    if (keylist == NULL) {
        db_status = tcrdbecode(rdb);
        if (format == txt_format) {
            db_error_to_txt(db_status, evb);
        } else {
            db_error_to_json(db_status, jsobj);
        }
    }
    
    list_count = tclistnum(keylist);
    for (i = off; keylist != NULL && i < (len+off) && i < list_count; i++) {
        kbuf = (char *)tclistval2(keylist, i);
        value = tcrdbget2(rdb, kbuf);
        if (value) {
            if (format == txt_format){
                evbuffer_add_printf(evb, "%s,%s\n", kbuf, value);
            } else {
                jsobj2 = json_object_new_object();
                json_object_object_add(jsobj2, kbuf, json_object_new_string(value));
                json_object_array_add(jsarr, jsobj2);
            }
            tcfree(value);
        }
    }
    tclistdel(keylist);
    
    if (format == json_format) {
        json_object_object_add(jsobj, "results", jsarr);
        json_object_object_add(jsobj, "status", json_object_new_string(list_count ? "ok" : "no results"));
    }
    
    finalize_request(req, evb, &args, jsobj);
}

void del_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key;
    struct evkeyvalq    args;
    struct json_object  *jsobj;
    
    if (rdb == NULL) {
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
    if (tcrdbout2(rdb, key)) {
        json_object_object_add(jsobj, "status", json_object_new_string("ok"));
    } else {
        db_status = tcrdbecode(rdb);
        db_error_to_json(db_status, jsobj);
    }
    
    finalize_request(req, evb, &args, jsobj);
}

void put_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key, *value;
    struct evkeyvalq    args;
    struct json_object  *jsobj;
    
    if (rdb == NULL) {
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
    if (tcrdbput2(rdb, key, value)) {
        json_object_object_add(jsobj, "status", json_object_new_string("ok"));
        json_object_object_add(jsobj, "value", json_object_new_string(value));
    } else {
        db_status = tcrdbecode(rdb);
        db_error_to_json(db_status, jsobj);
    }
    
    finalize_request(req, evb, &args, jsobj);
}

void get_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key, *value;
    struct evkeyvalq    args;
    struct json_object  *jsobj;
    
    if (rdb == NULL) {
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
    value = tcrdbget2(rdb, key);
    if (value) {
        json_object_object_add(jsobj, "status", json_object_new_string("ok"));
        json_object_object_add(jsobj, "value", json_object_new_string(value));
        free(value);
    } else {
        db_status = tcrdbecode(rdb);
        db_error_to_json(db_status, jsobj);
    }
    
    finalize_request(req, evb, &args, jsobj);
}

void get_int_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key;
    int                 *value;
    struct evkeyvalq    args;
    struct json_object  *jsobj;
    
    if (rdb == NULL) {
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
    value = (int *)tcrdbget2(rdb, key);
    if (value) {
        json_object_object_add(jsobj, "status", json_object_new_string("ok"));
        json_object_object_add(jsobj, "value", json_object_new_int((int) *value));
        free(value);
    } else {
        db_status = tcrdbecode(rdb);
        db_error_to_json(db_status, jsobj);
    }
    
    finalize_request(req, evb, &args, jsobj);
}

void mget_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key, *value;
    struct evkeyvalq    args;
    struct evkeyval *pair;
    struct json_object  *jsobj, *jserr;
    int nkeys = 0;
    
    if (rdb == NULL) {
        evhttp_send_error(req, 503, "database not connected");
        return;
    }
    
    evhttp_parse_query(req->uri, &args);
    
    jsobj = json_object_new_object();
    
    TAILQ_FOREACH(pair, &args, next) {
        if (pair->key[0] != 'k') continue;
        key = (char *)pair->value;
        nkeys++;
        
        value = tcrdbget2(rdb, key);
        if (value) {
            json_object_object_add(jsobj, key, json_object_new_string(value));
            free(value);
        } else {
            db_status = tcrdbecode(rdb);
            jserr = json_object_new_object();
            db_error_to_json(db_status, jserr);
            json_object_object_add(jsobj, key, jserr);
        }
    }
    
    if (!nkeys) {
        evhttp_send_error(req, 400, "key is required");
        evhttp_clear_headers(&args);
        return;
    }
    
    finalize_request(req, evb, &args, jsobj);
}

void mget_int_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key;
    int                 *value;
    struct evkeyvalq    args;
    struct evkeyval *pair;
    struct json_object  *jsobj, *jserr;
    int nkeys = 0;
    
    if (rdb == NULL) {
        evhttp_send_error(req, 503, "database not connected");
        return;
    }
    
    evhttp_parse_query(req->uri, &args);
    
    jsobj = json_object_new_object();
    
    TAILQ_FOREACH(pair, &args, next) {
        if (pair->key[0] != 'k') continue;
        key = (char *)pair->value;
        nkeys++;
        
        value = (int *)tcrdbget2(rdb, key);
        if (value) {
            json_object_object_add(jsobj, key, json_object_new_int((int)*value));
            free(value);
        } else {
            db_status = tcrdbecode(rdb);
            jserr = json_object_new_object();
            db_error_to_json(db_status, jserr);
            json_object_object_add(jsobj, key, jserr);
        }
    }
    
    if (!nkeys) {
        evhttp_send_error(req, 400, "key is required");
        evhttp_clear_headers(&args);
        return;
    }
    
    finalize_request(req, evb, &args, jsobj);
}

void incr_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *incr_value;
    struct evkeyvalq    args;
    struct json_object  *jsobj;
    int v;
    int value = 1;
    struct evkeyval *arg;
    bool has_key_arg = false;
    bool error = false;
    
    if (rdb == NULL) {
        evhttp_send_error(req, 503, "database not connected");
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
            if ((v = tcrdbaddint(rdb, arg->value, strlen(arg->value), value)) == INT_MIN ) {
                error = true;
                db_status = tcrdbecode(rdb);
                db_error_to_json(db_status, jsobj);
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
    
    finalize_request(req, evb, &args, jsobj);
}

void vanish_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct json_object  *jsobj;
    const char *json;
    
    if (rdb == NULL) {
        evhttp_send_error(req, 503, "database not connected");
        return;
    }
    
    tcrdbvanish(rdb);
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
    close_db(&rdb);
    fprintf(stdout, "/exit request recieved\n");
    event_loopbreak();
}

void info()
{
    fprintf(stdout, "simpletokyo: a light http interface to Tokyo Tyrant.\n");
    fprintf(stdout, "Version %s, https://github.com/bitly/simplehttp/tree/master/simpletokyo\n", VERSION);
}

void usage()
{
    fprintf(stderr, "%s: http wrapper for Tokyo Tyrant\n", NAME);
    fprintf(stderr, "\n");
    fprintf(stderr, "usage:\n");
    fprintf(stderr, "\t-A ttserver address\n");
    fprintf(stderr, "\t-P ttserver port\n");
    fprintf(stderr, "\t-a listen address\n");
    fprintf(stderr, "\t-p listen port\n");
    fprintf(stderr, "\t-D daemonize\n");
    fprintf(stderr, "\n");
    exit(1);
}

int main(int argc, char **argv)
{
    int ch;
    
    info();
    
    opterr=0;
    while ((ch = getopt(argc, argv, "A:P:h")) != -1) {
        if (ch == '?') {
            optind--; // re-set for next getopt() parse by simplehttp_init
            break;
        }
        switch (ch) {
        case 'A':
            db_host = optarg;
            break;
        case 'P':
            db_port = atoi(optarg);
            break;
        case 'h':
            usage();
            exit(1);
        }
    }
    
    memset(&db_status, -1, sizeof(db_status));
    
    simplehttp_init();
    db_reconnect(0, 0, NULL);
    simplehttp_set_cb("/get_int*", get_int_cb, NULL);
    simplehttp_set_cb("/get*", get_cb, NULL);
    simplehttp_set_cb("/mget_int*", mget_int_cb, NULL);
    simplehttp_set_cb("/mget*", mget_cb, NULL);
    simplehttp_set_cb("/put*", put_cb, NULL);
    simplehttp_set_cb("/del*", del_cb, NULL);
    simplehttp_set_cb("/vanish*", vanish_cb, NULL);
    simplehttp_set_cb("/fwmatch_int_merged*", fwmatch_int_merged_cb, NULL);
    simplehttp_set_cb("/fwmatch_int*", fwmatch_int_cb, NULL);
    simplehttp_set_cb("/fwmatch*", fwmatch_cb, NULL);
    simplehttp_set_cb("/incr*", incr_cb, NULL);
    simplehttp_set_cb("/stats*", stats_cb, NULL);
    simplehttp_set_cb("/exit", exit_cb, NULL);
    simplehttp_main(argc, argv);
    
    return 0;
}
