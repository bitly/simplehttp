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
#include "lib/timer.h"
#include "lib/util.h"

#define RECONNECT               5

#define NUM_REQUEST_TYPES       8
#define NUM_REQUESTS_FOR_STATS  1000
#define STATS_GET               0
#define STATS_GET_INT           1
#define STATS_PUT               2
#define STATS_INCR              3
#define STATS_DEL               4
#define STATS_FWMATCH           5
#define STATS_FWMATCH_INT       6
#define STATS_VANISH            7

void finalize_json(struct evhttp_request *req, struct evbuffer *evb, struct evkeyvalq *args, struct json_object *jsobj);
int open_db(char *addr, int port, TCRDB **rdb);
void close_db(TCRDB **rdb);
void db_reconnect(int fd, short what, void *ctx);
void argtoi(struct evkeyvalq *args, char *key, int *val, int def);
void db_error_to_json(int code, struct json_object *jsobj);
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

static char *version  = "1.4";
struct event ev;
struct timeval tv = {RECONNECT,0};
static char *db_host = "127.0.0.1";
static int db_port = 1978;
static TCRDB *rdb;
static int db_status;
static char *g_progname = "simpletokyo";

static uint64_t db_opened = 0;

static uint64_t requests = 0;
static uint64_t stats_request_counts[NUM_REQUEST_TYPES];
static int64_t stats_request[NUM_REQUESTS_FOR_STATS * NUM_REQUEST_TYPES];
static int stats_request_idx[NUM_REQUEST_TYPES];
static char *stats_request_labels[] = { "get", "get_int", "put", "incr", "del", "fwmatch", "fwmatch_int", "vanish" };

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

void close_db(TCRDB **rdb) {
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
    json_object_object_add(jsobj, "message", json_object_new_string((char *)tcrdberrmsg(code)));
}

void fwmatch_int_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key, *kbuf;
    int                 *value;
    int                 i, max, off, len;
    TCLIST              *keylist = NULL;
    struct evkeyvalq    args;
    struct json_object  *jsobj, *jsobj2, *jsarr;
    
    requests++;
    stats_request_counts[STATS_FWMATCH_INT]++;
    
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
      value = (int *)tcrdbget2(rdb, kbuf);
      if (value) {
          jsobj2 = json_object_new_object();
          json_object_object_add(jsobj2, kbuf, json_object_new_int((int) *value));
          json_object_array_add(jsarr, jsobj2);
          tcfree(value);
      }
    }
    if(keylist) tcfree(keylist);
    json_object_object_add(jsobj, "results", jsarr);
    
    if (keylist != NULL) {
        json_object_object_add(jsobj, "status", json_object_new_string("ok"));
    } else {
        db_status = tcrdbecode(rdb);
        db_error_to_json(db_status, jsobj);
    }

    finalize_json(req, evb, &args, jsobj);
}

void fwmatch_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key, *kbuf, *value;
    int                 i, max, off, len;
    TCLIST              *keylist = NULL;
    struct evkeyvalq    args;
    struct json_object  *jsobj, *jsobj2, *jsarr;
    
    requests++;
    stats_request_counts[STATS_FWMATCH]++;
    
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
    if(keylist) tcfree(keylist);
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
    
    _gettime(&ts1);
    
    requests++;
    stats_request_counts[STATS_DEL]++;
    
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
    
    _gettime(&ts2);
    stats_store_request(STATS_PUT, _ts_diff(ts1, ts2));
}

void get_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key, *value;
    struct evkeyvalq    args;
    struct json_object  *jsobj;
    
    _gettime(&ts1);
    
    requests++;
    stats_request_counts[STATS_GET]++;
    
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
    
    _gettime(&ts1);
    
    requests++;
    stats_request_counts[STATS_GET_INT]++;

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

    finalize_json(req, evb, &args, jsobj);
    
    _gettime(&ts2);
    stats_store_request(STATS_GET_INT, _ts_diff(ts1, ts2));
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
    
    _gettime(&ts1);
    
    requests++;
    stats_request_counts[STATS_INCR]++;

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

    finalize_json(req, evb, &args, jsobj);
    
    _gettime(&ts2);
    stats_store_request(STATS_INCR, _ts_diff(ts1, ts2));
}

void vanish_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct json_object  *jsobj;
    const char *json;
    
    _gettime(&ts1);

    requests++;
    stats_request_counts[STATS_VANISH]++;

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
    
    _gettime(&ts2);
    stats_store_request(STATS_VANISH, _ts_diff(ts1, ts2));
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
        evbuffer_add_printf(evb, "\"db_opens\": %"PRIu64",", db_opened);
        for (i = 0; i < NUM_REQUEST_TYPES; i++) {
            evbuffer_add_printf(evb, "\"%s_95\": %"PRIu64",", stats_request_labels[i], ninety_five_percents[i]);
            evbuffer_add_printf(evb, "\"%s_average_request\": %"PRIu64",", stats_request_labels[i], average_requests[i]);
            evbuffer_add_printf(evb, "\"%s_requests\": %"PRIu64",", stats_request_labels[i], stats_request_counts[i]);
        }
        evbuffer_add_printf(evb, "\"total_requests\": %"PRIu64, requests);
        evbuffer_add_printf(evb, "}\n");
    } else {
        evbuffer_add_printf(evb, "db opens: %"PRIu64"\n", db_opened);
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

void exit_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx) {
    close_db(&rdb);
    fprintf(stdout, "/exit request recieved\n");
    exit(0);
}

void info()
{
    fprintf(stdout, "simpletokyo: a light http interface to Tokyo Tyrant.\n");
    fprintf(stdout, "Version %s, https://github.com/bitly/simplehttp/tree/master/simpletokyo\n", version);
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
    
    memset(&stats_request, -1, sizeof(stats_request));
    memset(&stats_request_idx, 0, sizeof(stats_request_idx));
    memset(&stats_request_counts, 0, sizeof(stats_request_counts));
    
    memset(&db_status, -1, sizeof(db_status));
    
    simplehttp_init();
    db_reconnect(0, 0, NULL);
    simplehttp_set_cb("/get_int*", get_int_cb, NULL);
    simplehttp_set_cb("/get*", get_cb, NULL);
    simplehttp_set_cb("/put*", put_cb, NULL);
    simplehttp_set_cb("/del*", del_cb, NULL);
    simplehttp_set_cb("/vanish*", vanish_cb, NULL);
    simplehttp_set_cb("/fwmatch_int*", fwmatch_int_cb, NULL);
    simplehttp_set_cb("/fwmatch*", fwmatch_cb, NULL);
    simplehttp_set_cb("/incr*", incr_cb, NULL);
    simplehttp_set_cb("/stats*", stats_cb, NULL);
    simplehttp_set_cb("/exit", exit_cb, NULL);
    simplehttp_main(argc, argv);
    
    return 0;
}
