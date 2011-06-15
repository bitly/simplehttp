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

#define NAME            "simpletokyo"
#define VERSION         "2.0"

void finalize_request(int response_code, struct evhttp_request *req, struct evbuffer *evb, struct evkeyvalq *args, struct json_object *jsobj);
int db_open(char *addr, int port);
void db_close();
void db_reconnect();
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

static char *db_host = "127.0.0.1";
static int db_port = 1978;
static TCRDB *rdb = NULL;
static int db_status;
static uint64_t db_opened = 0;

void finalize_request(int response_code, struct evhttp_request *req, struct evbuffer *evb, struct evkeyvalq *args, struct json_object *jsobj)
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
        evhttp_send_reply(req, response_code, (response_code == HTTP_OK) ? "OK" : "ERROR", evb);
    }
    evhttp_clear_headers(args);
}

void db_close()
{
    int ecode=0;
    if (rdb != NULL) {
        fprintf(stderr, "closing db\n");
        if(!tcrdbclose(rdb)){
          ecode = tcrdbecode(rdb);
          fprintf(stderr, "close error: %s\n", tcrdberrmsg(ecode));
        }
        tcrdbdel(rdb);
        rdb = NULL;
    }
}

int db_open(char *addr, int port)
{
    db_opened++;
    int ecode=0;
    char *status;
    
    db_close();
    
    fprintf(stderr, "opening db %s:%d\n", addr, port);
    
    rdb = tcrdbnew();
    if (!tcrdbopen(rdb, addr, port)) {
        ecode = tcrdbecode(rdb);
        fprintf(stderr, "ERROR: %s:%d %s\n", addr, port, tcrdberrmsg(ecode));
        tcrdbdel(rdb);
        rdb = NULL;
    } else {
        status = tcrdbstat(rdb);
        fprintf(stderr, "%s---------------------\n", status);
        if (status) {
            tcfree(status);
        }
    }
    
    if (rdb == NULL) {
        fprintf(stderr, "db connection not open\n");
    }
    
    return ecode;
}

int db_should_reconnect(int status)
{
    return status == TTENOHOST || status == TTEREFUSED || status == TTESEND || status == TTERECV || status == TTEMISC;
}

void db_reconnect()
{
    db_status = db_open(db_host, db_port);
}

void db_error_to_json(int code, struct json_object *jsobj)
{
    // TTENOREC is the error code for not found - we dont need to log this
    if (code != TTENOREC) {
        fprintf(stderr, "error(%d): %s\n", code, tcrdberrmsg(code));
    }
    json_object_object_add(jsobj, "status", json_object_new_string("error"));
    json_object_object_add(jsobj, "code", json_object_new_int(code));
    json_object_object_add(jsobj, "message", json_object_new_string((char *)tcrdberrmsg(code)));
}

void db_error_to_txt(int code, struct evbuffer *evb)
{
    // TTENOREC is the error code for not found - we dont need to log this
    if (code != TTENOREC) {
        fprintf(stderr, "error(%d): %s\n", code, tcrdberrmsg(code));
    }
    if (EVBUFFER_LENGTH(evb)) {
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
    int response_code = HTTP_OK;
    bool retry = false;
    jsobj = NULL;
    jsarr = NULL;
    
    if (rdb == NULL) {
        db_reconnect();
        if (rdb == NULL) {
            evhttp_send_error(req, 503, "database not connected");
            return;
        }
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
        if (db_should_reconnect(db_status)) {
            db_reconnect();
            retry = true;
        }
    }
    
    // retry
    if (retry && ((keylist = tcrdbfwmkeys(rdb, key, strlen(key), max)) == NULL)) {
        db_status = tcrdbecode(rdb);
        if (db_should_reconnect(db_status)) {
            db_close();
        }
    }
    
    if (keylist) {
        list_count = tclistnum(keylist);
        for (i = off; i < (len+off) && i < list_count; i++) {
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
    } else {
        if (format == txt_format) {
            db_error_to_txt(db_status, evb);
        } else {
            db_error_to_json(db_status, jsobj);
        }
        response_code = 500;
    }
    
    finalize_request(response_code, req, evb, &args, jsobj);
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
    int response_code = HTTP_OK;
    bool retry = false;
    jsobj = NULL;
    jsarr = NULL;
    
    if (rdb == NULL) {
        db_reconnect();
        if (rdb == NULL) {
            evhttp_send_error(req, 503, "database not connected");
            return;
        }
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
        if (db_should_reconnect(db_status)) {
            db_reconnect();
            retry = true;
        }
    }
    
    // retry
    if (retry && ((keylist = tcrdbfwmkeys(rdb, key, strlen(key), max)) == NULL)) {
        db_status = tcrdbecode(rdb);
        if (db_should_reconnect(db_status)) {
            db_close();
        }
    }
    
    if (keylist) {
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
    } else {
        if (format == txt_format) {
            db_error_to_txt(db_status, evb);
        } else {
            db_error_to_json(db_status, jsobj);
        }
        response_code = 500;
    }
    
    finalize_request(response_code, req, evb, &args, jsobj);
}

void fwmatch_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key, *kbuf, *value;
    int                 i, max, off, len, list_count;
    int                 format;
    TCLIST              *keylist = NULL;
    struct evkeyvalq    args;
    struct json_object  *jsobj, *jsobj2, *jsarr;
    int response_code = HTTP_OK;
    bool retry = false;
    jsobj = NULL;
    jsarr = NULL;
    
    if (rdb == NULL) {
        db_reconnect();
        if (rdb == NULL) {
            evhttp_send_error(req, 503, "database not connected");
            return;
        }
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
        if (db_should_reconnect(db_status)) {
            db_reconnect();
            retry = true;
        }
    }
    
    // retry
    if (retry && ((keylist = tcrdbfwmkeys(rdb, key, strlen(key), max)) == NULL)) {
        db_status = tcrdbecode(rdb);
        if (db_should_reconnect(db_status)) {
            db_close();
        }
    }
    
    if (keylist) {
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
    } else {
        if (format == txt_format) {
            db_error_to_txt(db_status, evb);
        } else {
            db_error_to_json(db_status, jsobj);
        }
        response_code = 500;
    }
    
    finalize_request(response_code, req, evb, &args, jsobj);
}

void del_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key;
    struct evkeyvalq    args;
    struct json_object  *jsobj;
    int response_code = HTTP_OK;
    bool retry = false;
    int ret_code;
    
    if (rdb == NULL) {
        db_reconnect();
        if (rdb == NULL) {
            evhttp_send_error(req, 503, "database not connected");
            return;
        }
    }
    
    evhttp_parse_query(req->uri, &args);
    
    key = (char *)evhttp_find_header(&args, "key");
    if (key == NULL) {
        evhttp_send_error(req, 400, "key is required");
        evhttp_clear_headers(&args);
        return;
    }
    
    jsobj = json_object_new_object();
    
    if (!(ret_code = tcrdbout2(rdb, key))) {
        db_status = tcrdbecode(rdb);
        if (db_should_reconnect(db_status)) {
            db_reconnect();
            retry = true;
        }
    }
    
    // retry
    if (retry && !(ret_code = tcrdbout2(rdb, key))) {
        db_status = tcrdbecode(rdb);
        if (db_should_reconnect(db_status)) {
            db_close();
        }
    }
    
    if (ret_code) {
        json_object_object_add(jsobj, "status", json_object_new_string("ok"));
    } else {
        db_error_to_json(db_status, jsobj);
        response_code = 500;
    }
    
    finalize_request(response_code, req, evb, &args, jsobj);
}

void put_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key, *value;
    struct evkeyvalq    args;
    struct json_object  *jsobj;
    int response_code = HTTP_OK;
    bool retry = false;
    int ret_code;
    
    if (rdb == NULL) {
        db_reconnect();
        if (rdb == NULL) {
            evhttp_send_error(req, 503, "database not connected");
            return;
        }
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
    
    if (!(ret_code = tcrdbput2(rdb, key, value))) {
        db_status = tcrdbecode(rdb);
        if (db_should_reconnect(db_status)) {
            db_reconnect();
            retry = true;
        }
    }
    
    // retry
    if (retry && !(ret_code = tcrdbput2(rdb, key, value))) {
        db_status = tcrdbecode(rdb);
        if (db_should_reconnect(db_status)) {
            db_close();
        }
    }
    
    if (ret_code) {
        json_object_object_add(jsobj, "status", json_object_new_string("ok"));
        json_object_object_add(jsobj, "value", json_object_new_string(value));
    } else {
        db_error_to_json(db_status, jsobj);
        response_code = 500;
    }
    
    finalize_request(response_code, req, evb, &args, jsobj);
}

void get_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key, *value;
    struct evkeyvalq    args;
    struct json_object  *jsobj;
    int response_code = HTTP_OK;
    bool retry = false;
    
    if (rdb == NULL) {
        db_reconnect();
        if (rdb == NULL) {
            evhttp_send_error(req, 503, "database not connected");
            return;
        }
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
    if (!value) {
        db_status = tcrdbecode(rdb);
        if (db_should_reconnect(db_status)) {
            db_reconnect();
            retry = true;
        }
    }
    
    // retry
    if (retry && !(value = tcrdbget2(rdb, key))) {
        db_status = tcrdbecode(rdb);
        if (db_should_reconnect(db_status)) {
            db_close();
        }
    }
    
    if (value) {
        json_object_object_add(jsobj, "status", json_object_new_string("ok"));
        json_object_object_add(jsobj, "value", json_object_new_string(value));
        free(value);
    } else {
        db_error_to_json(db_status, jsobj);
        response_code = 500;
    }
    
    finalize_request(response_code, req, evb, &args, jsobj);
}

void get_int_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key;
    int                 *value;
    struct evkeyvalq    args;
    struct json_object  *jsobj;
    int response_code = HTTP_OK;
    bool retry = false;
    
    if (rdb == NULL) {
        db_reconnect();
        if (rdb == NULL) {
            evhttp_send_error(req, 503, "database not connected");
            return;
        }
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
    if (!value) {
        db_status = tcrdbecode(rdb);
        if (db_should_reconnect(db_status)) {
            db_reconnect();
            retry = true;
        }
    }
    
    // retry
    if (retry && !(value = (int *)tcrdbget2(rdb, key))) {
        db_status = tcrdbecode(rdb);
        if (db_should_reconnect(db_status)) {
            db_close();
        }
    }
    
    if (value) {
        json_object_object_add(jsobj, "status", json_object_new_string("ok"));
        json_object_object_add(jsobj, "value", json_object_new_int((int) *value));
        free(value);
    } else {
        db_error_to_json(db_status, jsobj);
        response_code = 500;
    }
    
    finalize_request(response_code, req, evb, &args, jsobj);
}

void mget_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key, *value;
    int                 format;
    struct evkeyvalq    args;
    struct evkeyval     *pair;
    struct json_object  *jsobj = NULL;
    int nkeys = 0;
    int response_code = HTTP_OK;
    bool retry;
    
    if (rdb == NULL) {
        db_reconnect();
        if (rdb == NULL) {
            evhttp_send_error(req, 503, "database not connected");
            return;
        }
    }
    
    evhttp_parse_query(req->uri, &args);
    format = get_argument_format(&args);
    
    if (format == json_format) {
        jsobj = json_object_new_object();
    }
    
    TAILQ_FOREACH(pair, &args, next) {
        if (pair->key[0] != 'k') continue;
        key = (char *)pair->value;
        nkeys++;
        retry = false;
        
        value = tcrdbget2(rdb, key);
        if (!value) {
            db_status = tcrdbecode(rdb);
            if (db_should_reconnect(db_status)) {
                db_reconnect();
                retry = true;
            }
        }
        
        // retry
        if (retry && !(value = tcrdbget2(rdb, key))) {
            db_status = tcrdbecode(rdb);
            
            if (db_status != TTENOREC) {
                // skip 404 errors on txt format; they just get no key,value line
                continue;
            }
            
            if (db_should_reconnect(db_status)) {
                db_close();
            }
            
            if (format == txt_format) {
                db_error_to_txt(db_status, evb);
            } else {
                db_error_to_json(db_status, jsobj);
            }
            
            response_code = 500;
            break;
        }
        
        if (value) {
            if (format == json_format) {
                json_object_object_add(jsobj, key, json_object_new_string(value));
            } else {
                evbuffer_add_printf(evb, "%s,%s\n", key, value);
            }
            free(value);
        }
    }
    
    if (!nkeys) {
        evhttp_send_error(req, 400, "key is required");
        evhttp_clear_headers(&args);
        json_object_put(jsobj);
        return;
    }
    
    finalize_request(response_code, req, evb, &args, jsobj);
}

void mget_int_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key;
    int                 *value;
    int                 format;
    struct evkeyvalq    args;
    struct evkeyval *pair;
    struct json_object *jsobj = NULL;
    int response_code = HTTP_OK;
    int nkeys = 0;
    bool retry;
    
    if (rdb == NULL) {
        db_reconnect();
        if (rdb == NULL) {
            evhttp_send_error(req, 503, "database not connected");
            return;
        }
    }
    
    evhttp_parse_query(req->uri, &args);
    format = get_argument_format(&args);
    
    if (format == json_format) {
        jsobj = json_object_new_object();
    }
    
    TAILQ_FOREACH(pair, &args, next) {
        if (pair->key[0] != 'k') continue;
        key = (char *)pair->value;
        nkeys++;
        retry = false;
        
        value = (int *)tcrdbget2(rdb, key);
        if (!value) {
            db_status = tcrdbecode(rdb);
            if (db_should_reconnect(db_status)) {
                db_reconnect();
                retry = true;
            }
        }
        
        if (retry && !(value = (int *)tcrdbget2(rdb, key))) {
            db_status = tcrdbecode(rdb);
            
            if (db_status != TTENOREC) {
                // skip 404 errors on txt format; they just get no key,value line
                continue;
            }
            
            if (db_should_reconnect(db_status)) {
                db_close();
            }
            
            if (format == txt_format) {
                db_error_to_txt(db_status, evb);
            } else {
                db_error_to_json(db_status, jsobj);
            }
            
            response_code = 500;
            break;
        }
        
        if (value) {
            if (format == json_format) {
                json_object_object_add(jsobj, key, json_object_new_int((int)*value));
            } else {
                evbuffer_add_printf(evb, "%s,%d\n", key, (int)*value);
            }
            free(value);
        }
    }
    
    if (!nkeys) {
        evhttp_send_error(req, 400, "key is required");
        evhttp_clear_headers(&args);
        json_object_put(jsobj);
        return;
    }
    
    finalize_request(response_code, req, evb, &args, jsobj);
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
    bool retry;
    int response_code = HTTP_OK;
    
    if (rdb == NULL) {
        db_reconnect();
        if (rdb == NULL) {
            evhttp_send_error(req, 503, "database not connected");
            return;
        }
    }
    
    evhttp_parse_query(req->uri, &args);
    
    incr_value = (char *)evhttp_find_header(&args, "value");
    
    if (incr_value != NULL) {
        value = atoi(incr_value);
    }
    
    jsobj = json_object_new_object();
    
    TAILQ_FOREACH(arg, &args, next) {
        if (strcasecmp(arg->key, "key") == 0) {
            retry = false;
            has_key_arg = true;
            
            if ((v = tcrdbaddint(rdb, arg->value, strlen(arg->value), value)) == INT_MIN) {
                db_status = tcrdbecode(rdb);
                if (db_should_reconnect(db_status)) {
                    db_reconnect();
                    retry = true;
                }
            }
            
            // retry
            if (retry && ((v = tcrdbaddint(rdb, arg->value, strlen(arg->value), value)) == INT_MIN)) {
                db_status = tcrdbecode(rdb);
                if (db_should_reconnect(db_status)) {
                    db_close();
                }
                error = true;
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
    
    if (!error) {
        json_object_object_add(jsobj, "status", json_object_new_string("ok"));
    } else {
        db_error_to_json(db_status, jsobj);
        response_code = 500;
    }
    
    finalize_request(response_code, req, evb, &args, jsobj);
}

void vanish_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct json_object  *jsobj;
    const char *json;
    
    if (rdb == NULL) {
        db_reconnect();
        if (rdb == NULL) {
            evhttp_send_error(req, 503, "database not connected");
            return;
        }
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
    fprintf(stdout, "/exit request recieved\n");
    event_loopbreak();
}

void info()
{
    fprintf(stdout, "simpletokyo: a light http interface to Tokyo Tyrant.\n");
    fprintf(stdout, "Version: %s, https://github.com/bitly/simplehttp/tree/master/simpletokyo\n", VERSION);
}

int version_cb(int value) {
    fprintf(stdout, "Version: %s\n", VERSION);
    return 0;
}

int main(int argc, char **argv)
{
    define_simplehttp_options();
    option_define_str("ttserver_host", OPT_OPTIONAL, "127.0.0.1", &db_host, NULL, NULL);
    option_define_int("ttserver_port", OPT_OPTIONAL, 1978, &db_port, NULL, NULL);
    option_define_bool("version", OPT_OPTIONAL, 0, NULL, version_cb, VERSION);
    
    if (!option_parse_command_line(argc, argv)){
        return 1;
    }
    
    info();
    
    db_status = -1;
    db_reconnect();
    
    simplehttp_init();
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
    simplehttp_main();
    
    db_close();
    free_options();
    
    return 0;
}
