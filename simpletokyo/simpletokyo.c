#include <tcrdb.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <simplehttp/queue.h> 
#include <simplehttp/simplehttp.h>
#include "json/json.h"


#define RECONNECT 5
#define MAXRES 1000

void finalize_json(struct evhttp_request *req, struct evbuffer *evb, 
                    struct evkeyvalq *args, struct json_object *jsobj);
int open_db(char *addr, int port, TCRDB **rdb);
void db_reconnect(int fd, short what, void *ctx);
void argtoi(struct evkeyvalq *args, char *key, int *val, int def);
void db_error_to_json(int code, struct json_object *jsobj);
void fwmatch_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void del_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void put_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void get_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void get_int_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void incr_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);

struct event ev;
struct timeval tv = {RECONNECT,0};
static char *db_host = "127.0.0.1";
static int db_port = 1978;
static TCRDB *rdb;
static int db_status;
static char *g_progname = "simpletokyo";


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
    int s;
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

void fwmatch_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key, *kbuf, *value;
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

    finalize_json(req, evb, &args, jsobj);
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

    finalize_json(req, evb, &args, jsobj);
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
    if (value = tcrdbget2(rdb, key)) {
        json_object_object_add(jsobj, "status", json_object_new_string("ok"));
        json_object_object_add(jsobj, "value", json_object_new_string(value));
        free(value);
    } else {
        db_status = tcrdbecode(rdb);
        db_error_to_json(db_status, jsobj);
    }

    finalize_json(req, evb, &args, jsobj);
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
    if (value = (int *)tcrdbget2(rdb, key)) {
        json_object_object_add(jsobj, "status", json_object_new_string("ok"));
        json_object_object_add(jsobj, "value", json_object_new_int((int) *value));
        free(value);
    } else {
        db_status = tcrdbecode(rdb);
        db_error_to_json(db_status, jsobj);
    }

    finalize_json(req, evb, &args, jsobj);
}
void incr_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key, *incr_value;
    struct evkeyvalq    args;
    struct json_object  *jsobj;
    int v;
    int value = 1;

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
    incr_value = (char *)evhttp_find_header(&args, "value");
    if (incr_value != NULL) {
        value = atoi(incr_value);
    }
    
    jsobj = json_object_new_object();
    if ((v = tcrdbaddint(rdb, key, strlen(key), value)) == INT_MIN ) {
      db_status = tcrdbecode(rdb);
      db_error_to_json(db_status, jsobj);
    } else {
      json_object_object_add(jsobj, "status", json_object_new_string("ok"));
      json_object_object_add(jsobj, "value", json_object_new_int(value));
    }

    finalize_json(req, evb, &args, jsobj);
}

void usage()
{
    fprintf(stderr, "%s: http wrapper for Tokyo Tyrant\n", g_progname);
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

int
main(int argc, char **argv)
{
    int ch;
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
    simplehttp_set_cb("/put*", put_cb, NULL);
    simplehttp_set_cb("/del*", del_cb, NULL);
    simplehttp_set_cb("/fwmatch*", fwmatch_cb, NULL);
    simplehttp_set_cb("/incr*", incr_cb, NULL);
    simplehttp_main(argc, argv);

    return 0;
}
