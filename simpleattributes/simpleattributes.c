#include <tcrdb.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <simplehttp/queue.h>
#include <simplehttp/simplehttp.h>
#include <json/json.h>

#define RECONNECT 5
#define MAXRES 1000

void finalize_json(struct evhttp_request *req, struct evbuffer *evb, 
                    struct evkeyvalq *args, struct json_object *jsobj);
int open_db(char *addr, int port, TCRDB **rdb);
void db_reconnect(int fd, short what, void *ctx);
void db_error_to_json(int code, struct json_object *jsobj);
void idx_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void del_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void put_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void get_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);

struct event ev;
struct timeval tv = {RECONNECT,0};
char *db_host = "127.0.0.1";
int db_port = 1978;
TCRDB *rdb;
int db_status;


void finalize_json(struct evhttp_request *req, struct evbuffer *evb, 
                    struct evkeyvalq *args, struct json_object *jsobj)
{
    char *json, *jsonp;
    
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

int open_db(char *addr, int port, TCRDB **rdb)
{
    int ecode=0;

    if (*rdb != NULL) {
        if(!tcrdbclose(*rdb)){
          ecode = tcrdbecode(*rdb);
          fprintf(stderr, "close error: %s\n", tcrdberrmsg(ecode));
        }
        tcrdbdel(*rdb);
        *rdb = NULL;
    }
    *rdb = tcrdbnew();
    if(!tcrdbopen(*rdb, addr, port)){
        ecode = tcrdbecode(*rdb);
        fprintf(stderr, "open error(%s:%d): %s\n", addr, port, tcrdberrmsg(ecode));
        *rdb = NULL;
    } else {
        char *status = tcrdbstat(*rdb);
        printf("%s---------------------\n", status);
        if (status) free(status);
    }
    return ecode;
}

void db_reconnect(int fd, short what, void *ctx)
{
    int i, s;

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
    fprintf(stderr, "error(%d): %s\n", code, tcrdberrmsg(code));
    json_object_object_add(jsobj, "status", json_object_new_string("error"));
    json_object_object_add(jsobj, "code", json_object_new_int(code));
    json_object_object_add(jsobj, "message",
                            json_object_new_string((char *)tcrdberrmsg(code)));
}

void idx_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *json, *key, *kbuf, *value;
    int                 i, max, off, len;
    TCLIST              *keylist = NULL;
    struct evkeyvalq    args;
    struct json_object  *jsobj, *jsobj2, *jsarr;

    if (rdb == NULL) {
        evhttp_send_error(req, 503, "database not connected");
        return;
    }
    evhttp_parse_query(req->uri, &args);

    key = (char *)evhttp_find_header(&args, "key");
    max = get_int_argument(&args, "max", 1000);
    len = get_int_argument(&args, "length", 10);
    off = get_int_argument(&args, "offset", 0);
    if (key == NULL) {
        evhttp_send_error(req, 400, "key is required");
        evhttp_clear_headers(&args);
        return;
    }
    
    jsobj = json_object_new_object();
    jsarr = json_object_new_array();
    
    keylist = tcrdbfwmkeys2(rdb, key, max);
    for (i=off; keylist!=NULL && i<(len+off) && i<tclistnum(keylist); i++){
      kbuf = (char *)tclistval2(keylist, i);
      value = tcrdbget2(rdb, kbuf);
      if (value) {
          jsobj2 = json_object_new_object();
          json_object_object_add(jsobj2, kbuf, json_object_new_string(value));
          json_object_array_add(jsarr, jsobj2);
          tcfree(value);
      }
    }
    if (keylist) tcfree(keylist);
    json_object_object_add(jsobj, "results", jsarr);
    
    if (keylist != NULL) {
        json_object_object_add(jsobj, "status", json_object_new_string("ok"));
    } else {
        db_status = tcrdbecode(rdb);
        db_error_to_json(db_status, jsobj);
    }

    finalize_json(req, evb, &args, jsobj);
}

void del_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *json, *key;
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

    finalize_json(req, evb, &args, jsobj);
}

void put_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    int i;
    char *json, *hash, *kvs, *key, *value;
    struct evkeyvalq args;
    struct json_object *jsobj, *jsonPtr, *jsonPtr2;
    TCMAP *cols;

    if (rdb == NULL) {
        evhttp_send_error(req, 503, "database not connected");
        return;
    }
    evhttp_parse_query(req->uri, &args);

    hash = (char *)evhttp_find_header(&args, "hash");
    kvs = (char *)evhttp_find_header(&args, "kvs");
    
    if (hash == NULL) {
        evhttp_send_error(req, 400, "hash is required");
        evhttp_clear_headers(&args);
        return;
    }
    if (kvs == NULL) {
        evhttp_send_error(req, 400, "kvs is required");
        evhttp_clear_headers(&args);
        return;
    }
    
    jsonPtr = json_tokener_parse(kvs);
    if (!jsonPtr) {
        evhttp_send_error(req, 400, "kvs json is invalid");
        evhttp_clear_headers(&args);
        return;
    }
    
    cols = tcmapnew();
        
    for (i=0; i < json_object_array_length(jsonPtr); i++) {
        jsonPtr2 = json_object_array_get_idx(jsonPtr, i);
        key = (char *)json_object_get_string(json_object_array_get_idx(jsonPtr2, 0));
        value = (char *)json_object_get_string(json_object_array_get_idx(jsonPtr2, 1));
        tcmapput2(cols, key, value);
    }
    
    jsobj = json_object_new_object();
    
    if (tcrdbtblput(rdb, hash, sizeof(hash), cols)) {
        json_object_object_add(jsobj, "status", json_object_new_string("ok"));
    } else {
        db_status = tcrdbecode(rdb);
        db_error_to_json(db_status, jsobj);
    }
    
    tcmapdel(cols);

    finalize_json(req, evb, &args, jsobj);
}

void get_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char *json, *hash, *key, *value, *name;
    struct evkeyvalq args;
    struct json_object *jsobj, *jsobj2, *jsobj3;
    TCMAP *cols;
    
    if (rdb == NULL) {
        evhttp_send_error(req, 503, "database not connected");
        return;
    }

    evhttp_parse_query(req->uri, &args);

    hash = (char *)evhttp_find_header(&args, "hash");
    key = (char *)evhttp_find_header(&args, "key");
    
    if (hash == NULL) {
        evhttp_send_error(req, 400, "hash is required");
        evhttp_clear_headers(&args);
        return;
    }
    
    jsobj = json_object_new_object();
    jsobj2 = json_object_new_object();
    cols = tcrdbtblget(rdb, hash, sizeof(hash));
    
    if (cols) {
        tcmapiterinit(cols);
        jsobj3 = json_object_new_object();
        
        
        if (key) {
            value = (char *)tcmapget2(cols, key);

            if (!value) {
                value = "";
            }
            
            json_object_object_add(jsobj2, key, json_object_new_string(value));
        } else {
            while ((name = (char *)tcmapiternext2(cols)) != NULL) {
                json_object_object_add(jsobj2, name, json_object_new_string(tcmapget2(cols, name)));
            }
        }
     
        json_object_object_add(jsobj, "status", json_object_new_string("ok"));
        json_object_object_add(jsobj, "results", jsobj2);
        
        tcmapdel(cols);
    } else {
        json_object_object_add(jsobj, "status", json_object_new_string("error"));
    }
   
    finalize_json(req, evb, &args, jsobj);
}


int
main(int argc, char **argv)
{   
    define_simplehttp_options();
    option_define_str("ttserver_host", OPT_OPTIONAL, "127.0.0.1", &db_host, NULL, NULL);
    option_define_int("ttserver_port", OPT_OPTIONAL, 1978, &db_port, NULL, NULL);
    if (!option_parse_command_line(argc, argv)){
        return 1;
    }
    
    memset(&db_status, -1, sizeof(db_status));
    simplehttp_init();
    db_reconnect(0, 0, NULL);
    simplehttp_set_cb("/get*", get_cb, NULL);
    simplehttp_set_cb("/put*", put_cb, NULL);
    simplehttp_set_cb("/del*", del_cb, NULL);
    simplehttp_set_cb("/idx*", idx_cb, NULL);
    simplehttp_main();
    free_options();

    return 0;
}
