#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>
#include <string.h>
#include "queue.h"
#include "simplehttp.h"
#include "async_simplehttp.h"
#include "request.h"
#include "stat.h"

extern int simplehttp_logging;

struct simplehttp_request *simplehttp_request_new(struct evhttp_request *req, uint64_t id)
{
    struct simplehttp_request *s_req;
    simplehttp_ts start_ts;
    
    simplehttp_ts_get(&start_ts);
    s_req = malloc(sizeof(struct simplehttp_request));
    s_req->req = req;
    s_req->start_ts = start_ts;
    s_req->id = id;
    s_req->async = 0;
    s_req->index = -1;
    TAILQ_INSERT_TAIL(&simplehttp_reqs, s_req, entries);
    
    AS_DEBUG("simplehttp_request_new (%p)\n", s_req);
    
    return s_req;
}

struct simplehttp_request *simplehttp_request_get(struct evhttp_request *req)
{
    struct simplehttp_request *entry;
    
    TAILQ_FOREACH(entry, &simplehttp_reqs, entries) {
        if (req == entry->req) {
            return entry;
        }
    }
    
    return NULL;
}

uint64_t simplehttp_request_id(struct evhttp_request *req)
{
    struct simplehttp_request *entry;
     
    entry = simplehttp_request_get(req);
    
    return entry ? entry->id : 0;
}

struct simplehttp_request *simplehttp_async_check(struct evhttp_request *req)
{
    struct simplehttp_request *entry;
    
    entry = simplehttp_request_get(req);
    if (entry && entry->async) {
        return entry;
    }
    
    return NULL;
}

void simplehttp_async_enable(struct evhttp_request *req)
{
    struct simplehttp_request *entry;
    
    if ((entry = simplehttp_request_get(req)) != NULL) {
        AS_DEBUG("simplehttp_async_enable (%p)\n", req);
        entry->async = 1;
    }
}

void simplehttp_request_finish(struct evhttp_request *req, struct simplehttp_request *s_req)
{
    simplehttp_ts end_ts;
    uint64_t req_time;
    char id_buf[64];
    
    AS_DEBUG("simplehttp_request_finish (%p, %p)\n", req, s_req);
    
    simplehttp_ts_get(&end_ts);
    req_time = simplehttp_ts_diff(s_req->start_ts, end_ts);
    
    if (s_req->index != -1) {
        simplehttp_stats_store(s_req->index, req_time);
    }
    
    if (simplehttp_logging) {
        sprintf(id_buf, "%"PRIu64, s_req->id);
        simplehttp_log("", req, req_time, id_buf);
    }
    
    AS_DEBUG("\n");
    
    TAILQ_REMOVE(&simplehttp_reqs, s_req, entries);
    free(s_req);
}

void simplehttp_async_finish(struct evhttp_request *req)
{
    struct simplehttp_request *entry;
    
    AS_DEBUG("simplehttp_async_finish (%p)\n", req);
    if ((entry = simplehttp_async_check(req))) {
        AS_DEBUG("simplehttp_async_check found (%p)\n", entry);
        simplehttp_request_finish(req, entry);
    }
}


int get_argument_format(struct evkeyvalq *args)
{
    int format_code = json_format;
    char *format = (char *)evhttp_find_header(args, "format");
    if (format && !strncmp(format, "txt", 3)) {
        format_code = txt_format;
    }
    return format_code;
}

int get_int_argument(struct evkeyvalq *args, char *key, int default_value)
{
    char *tmp;
    if (!key) return default_value;
    tmp = (char *)evhttp_find_header(args, (const char *)key);
    if (tmp) {
        return atoi(tmp);
    }
    return default_value;
}

double get_double_argument(struct evkeyvalq *args, char *key, double default_value)
{
    char *tmp;
    if (!key) return default_value;
    tmp = (char *)evhttp_find_header(args, (const char *)key);
    if (tmp) {
        return atof(tmp);
    }
    return default_value;
}

