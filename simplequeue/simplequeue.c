#include <sys/time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tcutil.h>
#include <tcfdb.h>

#include "queue.h"
#include "simplehttp.h"
#include "json.h"

#define MAXRES 1000
#define BUFSZ 1024

void finalize_json(struct evhttp_request *req, struct evbuffer *evb, 
                    struct evkeyvalq *args, struct json_object *jsobj);
void argtoi(struct evkeyvalq *args, char *key, int *val, int def);
void db_error_to_json(int code, struct json_object *jsobj);

static TCFDB *fdb;

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
    struct evkeyvalq *args;
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

int
main(int argc, char **argv)
{	
    int ecode;
    char *db;
    
	if (argc != 1) {
        fprintf(stderr, "usage: %s db\n", argv[0]);
	}
    
    db = argv[1];
    fdb = tcfdbnew();
    
    if (!tcfdbopen(fdb, db, FDBOWRITER | FDBOCREAT)) {
        ecode = tcfdbecode(fdb);
        fprintf(stderr, "open error: %s\n", tcfdberrmsg(ecode));
    }   
    
    simplehttp_init();
    simplehttp_set_cb("/put*", put, NULL);
    simplehttp_set_cb("/get*", get, NULL);
    simplehttp_set_cb("/stats*", stats, NULL);
    simplehttp_main(argc, argv);
    
    return 0;
}
