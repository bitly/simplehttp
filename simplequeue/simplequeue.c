#include <sys/time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tcrdb.h>

#include "queue.h"
#include "simplehttp.h"
#include "json.h"

#define RECONNECT 5
#define MAXRES 1000
#define BUFSZ 1024

void finalize_json(struct evhttp_request *req, struct evbuffer *evb, 
                    struct evkeyvalq *args, struct json_object *jsobj);
int open_db(char *addr, int port, TCRDB **rdb);
void db_reconnect(int fd, short what, void *ctx);
void argtoi(struct evkeyvalq *args, char *key, int *val, int def);
void db_error_to_json(int code, struct json_object *jsobj);
int incr_stat(TCRDB *rdb, char *queue, char *key);
int decr_stat(TCRDB *rdb, char *queue, char *key);
char *get_stat(TCRDB *rdb, char *queue, char *key);

struct event ev;
struct timeval tv = {RECONNECT,0};
static char *db_host = "0.0.0.0";
static int db_port = 1978;
static TCRDB *rdb;
static int db_status;
static char *g_progname;

uint32_t depth = 0;
uint32_t depth_high_water = 0;
uint32_t n_puts = 0;
uint32_t n_gets = 0;

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
    json_object_object_add(jsobj, "message", json_object_new_string((char *)tcrdberrmsg(code)));
}

void
stats(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct evkeyvalq args;
	struct json_object *jsobj;
	int reset;
    char *uri, *queue, *total_gets, *total_puts, *total;
    char kbuf[BUFSZ];
    
    uri = evhttp_decode_uri(req->uri);
    evhttp_parse_query(uri, &args);
    free(uri);

	argtoi(&args, "reset", &reset, 0);
	jsobj = json_object_new_object();
	json_object_object_add(jsobj, "puts", json_object_new_int(n_puts));
	json_object_object_add(jsobj, "gets", json_object_new_int(n_gets));
	json_object_object_add(jsobj, "depth", json_object_new_int(depth));
	json_object_object_add(jsobj, "maxDepth", json_object_new_int(depth_high_water));
	
	queue = (char *)evhttp_find_header(&args, "queue");
	if (queue != NULL) {
        sprintf(kbuf, "%s.gets", queue);
        total_gets = get_stat(rdb, queue, "gets");
        if (total_gets == NULL) {
            json_object_object_add(jsobj, kbuf, json_object_new_int(0));
        } else {
            json_object_object_add(jsobj, kbuf, json_object_new_int(atoi(total_gets)));
            tcfree(total_gets);
        }
        
        sprintf(kbuf, "%s.puts", queue);
        total_puts = get_stat(rdb, queue, "puts");
        if (total_puts == NULL) {
            json_object_object_add(jsobj, kbuf, json_object_new_int(0));
        } else {
            json_object_object_add(jsobj, kbuf, json_object_new_int(atoi(total_puts)));
            tcfree(total_puts);
        }
        
        sprintf(kbuf, "%s.depth", queue);
        total = get_stat(rdb, queue, "depth");
        if (total == NULL) {
            json_object_object_add(jsobj, kbuf, json_object_new_int(0));
        } else {
            json_object_object_add(jsobj, kbuf, json_object_new_int(atoi(total)));
            tcfree(total);
        }
	}
	
	if (reset) {
        depth_high_water = 0;
        n_puts = 0;
        n_gets = 0;
    } 

	finalize_json(req, evb, &args, jsobj);
}   

void
get(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char *queue, *kbuf, *value, *uri;
    int peek;
    TCLIST *keylist = NULL;
    struct evkeyvalq args;
    struct json_object *jsobj;
	char key[BUFSZ];

	if (rdb == NULL) {
        evhttp_send_error(req, 503, "database not connected");
        return;
    }

	uri = evhttp_decode_uri(req->uri);
    evhttp_parse_query(uri, &args);
    free(uri);
	
	argtoi(&args, "peek", &peek, 0);
	queue = (char *)evhttp_find_header(&args, "queue");
	if (queue == NULL) {
        evhttp_send_error(req, 400, "queue is required");
        evhttp_clear_headers(&args);
        return;
    }

	jsobj = json_object_new_object();
	sprintf(key, "%s\t", queue);
    keylist = tcrdbfwmkeys2(rdb, key, 1);
	if (keylist != NULL) {
		kbuf = (char *)tclistval2(keylist, 0);
		
		if (kbuf == NULL) {
			json_object_object_add(jsobj, "status", json_object_new_string("ok"));
			goto done;
		}
		
	    value = tcrdbget2(rdb, kbuf);
	
		if (value != NULL) {
			json_object_object_add(jsobj, "queue", json_object_new_string(queue));
	        json_object_object_add(jsobj, "value", json_object_new_string(value));
	        tcfree(value);
	
			if (!peek) {
				tcrdbout2(rdb, kbuf);
			}
			
			n_gets++;
			depth--;
			decr_stat(rdb, queue, "total");
            incr_stat(rdb, queue, "gets");
			
			if (depth < 0) {
				depth = 0;
			}
		}		
	} 
	
    if (keylist != NULL) {
        json_object_object_add(jsobj, "status", json_object_new_string("ok"));
    } else {
        db_status = tcrdbecode(rdb);
        db_error_to_json(db_status, jsobj);
    }

done:
    tcfree(keylist); 
	finalize_json(req, evb, &args, jsobj);
}

int
incr_stat(TCRDB *rdb, char *queue, char *key) {
    int len;
    char kbuf[BUFSZ];
    
    len = sprintf(kbuf, "s.%s.%s", queue, key);
    
    return tcrdbaddint(rdb, kbuf, len+1, 1);
}

int
decr_stat(TCRDB *rdb, char *queue, char *key) {
    int len;
    char kbuf[BUFSZ];
    
    len = sprintf(kbuf, "s.%s.%s", queue, key);
    
    return tcrdbaddint(rdb, kbuf, len+1, -1);
}

char *
get_stat(TCRDB *rdb, char *queue, char *key) {
    char kbuf[BUFSZ];
    
    sprintf(kbuf, "s.%s.%s", queue, key);
    
    return tcrdbget2(rdb, kbuf);
}
    
void
put(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct evkeyvalq args;
	struct json_object *jsobj;
	static struct timeval timestamp;
    char *queue, *value, *uri;
	char key[BUFSZ];

	if (rdb == NULL) {
        evhttp_send_error(req, 503, "database not connected");
        return;
    }
    
    uri = evhttp_decode_uri(req->uri);
    evhttp_parse_query(uri, &args);
    free(uri);

	queue = (char *)evhttp_find_header(&args, "queue");
	value = (char *)evhttp_find_header(&args, "value");
    
    if (queue == NULL) {
        evhttp_send_error(req, 400, "queue is required");
        evhttp_clear_headers(&args);
        return;
    }
    if (value == NULL) {
        evhttp_send_error(req, 400, "value is required");
        evhttp_clear_headers(&args);
        return;
    }

	gettimeofday(&timestamp, NULL);
	sprintf(key, "%s\t%d.%d", queue, (int)timestamp.tv_sec, (int)timestamp.tv_usec);
	
	n_puts++;
    depth++;
    incr_stat(rdb, queue, "depth");
    incr_stat(rdb, queue, "puts");
    
    if (depth > depth_high_water) {
        depth_high_water = depth;
    }
    
   	jsobj = json_object_new_object();
    if (tcrdbput2(rdb, key, value)) {
        json_object_object_add(jsobj, "status", json_object_new_string("ok"));
    } else {
        db_status = tcrdbecode(rdb);
        db_error_to_json(db_status, jsobj);
    }

    finalize_json(req, evb, &args, jsobj);
}

void usage()
{
    fprintf(stderr, "%s: HTTP wrapper for Tokyo Tyrant\n", g_progname);
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
    simplehttp_set_cb("/put*", put, NULL);
    simplehttp_set_cb("/get*", get, NULL);
    simplehttp_set_cb("/stats*", stats, NULL);
    simplehttp_main(argc, argv);
    
    return 0;
}