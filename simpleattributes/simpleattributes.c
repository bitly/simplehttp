#include <tcrdb.h>
#include "queue.h"
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include "simplehttp.h"
#include "json/json.h"

#define RECONNECT 5
#define MAXRES 1000

void finalize_json(struct evhttp_request *req, struct evbuffer *evb, 
                    struct evkeyvalq *args, struct json_object *jsobj);
int open_db(char *addr, int port, TCRDB **rdb);
void db_reconnect(int fd, short what, void *ctx);
void argtoi(struct evkeyvalq *args, char *key, int *val, int def);
void db_error_to_json(int code, struct json_object *jsobj);
void idx_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void del_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void put_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void get_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);

struct event ev;
struct timeval tv = {RECONNECT,0};
static char *db_host = "0.0.0.0";
static int db_port = 1978;
static TCRDB *rdb;
static int db_status;
static char *g_progname;


void finalize_json(struct evhttp_request *req, struct evbuffer *evb, 
                    struct evkeyvalq *args, struct json_object *jsobj)
{
    char *json, *jsonp;
    
    jsonp = (char *)evhttp_find_header(args, "jsonp");
    json = json_object_to_json_string(jsobj);
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

void argtoi(struct evkeyvalq *args, char *key, int *val, int def)
{
    char *tmp;

    *val = def;
    tmp = (char *)evhttp_find_header(args, (const char *)key);
    if (tmp) {
        *val = atoi(tmp);
    }
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
    char                *uri, *json, *key, *kbuf, *value;
    int                 i, max, off, len;
    TCLIST              *keylist = NULL;
    struct evkeyvalq    args;
    struct json_object  *jsobj, *jsobj2, *jsarr;

    if (rdb == NULL) {
        evhttp_send_error(req, 503, "database not connected");
        return;
    }
    uri = evhttp_decode_uri(req->uri);
    evhttp_parse_query(uri, &args);
    free(uri);

    key = (char *)evhttp_find_header(&args, "key");
    argtoi(&args, "max", &max, 1000);
    argtoi(&args, "length", &len, 10);
    argtoi(&args, "offset", &off, 0);
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
    char                *uri, *json, *key;
    struct evkeyvalq    args;
    struct json_object  *jsobj;

    if (rdb == NULL) {
        evhttp_send_error(req, 503, "database not connected");
        return;
    }
    uri = evhttp_decode_uri(req->uri);
    evhttp_parse_query(uri, &args);
    free(uri);

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
    char *uri, *json, *hash, *kvs, *key, *value;
    struct evkeyvalq args;
    struct json_object *jsobj, *jsonPtr, *jsonPtr2;
    TCMAP *cols;

    if (rdb == NULL) {
        evhttp_send_error(req, 503, "database not connected");
        return;
    }
    uri = evhttp_decode_uri(req->uri);
    evhttp_parse_query(uri, &args);
    free(uri);

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
    
    if (jsonPtr == 0xfffffffffffffffc) {
        evhttp_send_error(req, 400, "kvs json is invalid");
        evhttp_clear_headers(&args);
        return;
    }
    
    cols = tcmapnew();
        
    for (i=0; i < json_object_array_length(jsonPtr); i++) {
        jsonPtr2 = json_object_array_get_idx(jsonPtr, i);
        key = json_object_get_string(json_object_array_get_idx(jsonPtr2, 0));
        value = json_object_get_string(json_object_array_get_idx(jsonPtr2, 1));
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
    char *uri, *json, *hash, *key, *value, *name;
    struct evkeyvalq args;
    struct json_object *jsobj, *jsobj2, *jsobj3;
    TCMAP *cols;
    
    if (rdb == NULL) {
        evhttp_send_error(req, 503, "database not connected");
        return;
    }

    uri = evhttp_decode_uri(req->uri);
    evhttp_parse_query(uri, &args);
    free(uri);

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
            value = tcmapget2(cols, key);

            if (!value) {
                value = "";
            }
            
            json_object_object_add(jsobj2, key, json_object_new_string(value));
        } else {
            while ((name = tcmapiternext2(cols)) != NULL) {
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

void usage()
{
    fprintf(stderr, "%s: http wrapper for Tokyo Tyrant\n", g_progname);
    fprintf(stderr, "\n");
    fprintf(stderr, "usage:\n");
    fprintf(stderr, "  %s [-tchost 0.0.0.0] [-tcport 1978]\n", g_progname);
    fprintf(stderr, "\n");
    exit(1);
}

int
main(int argc, char **argv)
{   
    int i;
    
    g_progname = argv[0];
    for (i=1; i < argc; i++) {
        if(!strcmp(argv[i], "-tchost")) {
            if(++i >= argc) usage();
            db_host = argv[i];
        } else if(!strcmp(argv[i], "-tcport")) {
            if(++i >= argc) usage();
            db_port = tcatoi(argv[i]);
        } else if (!strcmp(argv[i], "-help")) {
            usage();
        }
    }
    
    memset(&db_status, -1, sizeof(db_status));
    simplehttp_init();
    db_reconnect(0, 0, NULL);
    simplehttp_set_cb("/get*", get_cb, NULL);
    simplehttp_set_cb("/put*", put_cb, NULL);
    simplehttp_set_cb("/del*", del_cb, NULL);
    simplehttp_set_cb("/idx*", idx_cb, NULL);
    simplehttp_main(argc, argv);

    return 0;
}
