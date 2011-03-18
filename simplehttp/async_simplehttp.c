#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <inttypes.h>
#include <time.h>
#include "async_simplehttp.h"

static int request_logging = 0;

TAILQ_HEAD(, Connection) connection_pool;

void init_async_connection_pool(int enable_request_logging)
{
    request_logging = enable_request_logging;
    TAILQ_INIT(&connection_pool);
}

void free_async_connection_pool()
{
    int i;
    struct Connection *conn;
    
    while ((conn = TAILQ_FIRST(&connection_pool))) {
        TAILQ_REMOVE(&connection_pool, conn, next);
        for (i=0; i< ASYNC_PER_HOST_CONNECTION_LIMIT; i++) {
            evhttp_connection_free(conn->evcon[i]);
        }
        free(conn->address);
        free(conn);
    }
}

struct evhttp_connection *get_connection(char *address, int port, struct Connection **store_conn)
{
    struct Connection *conn;
    int i;
    
    TAILQ_FOREACH(conn, &connection_pool, next) {
        if ((strcmp(conn->address, address) == 0) && (conn->port == port)) {
            *store_conn = conn;
            return conn->evcon[conn->next_evcon++ % ASYNC_PER_HOST_CONNECTION_LIMIT];
        }
    }
    conn = malloc(sizeof(struct Connection));
    conn->address = strdup(address);
    conn->port = port;
    conn->next_evcon = 0;
    for (i=0; i< ASYNC_PER_HOST_CONNECTION_LIMIT; i++){
        conn->evcon[i] = evhttp_connection_new(address, port);
        evhttp_connection_set_retries(conn->evcon[i], 0);
    }
    TAILQ_INSERT_TAIL(&connection_pool, conn, next);
    *store_conn = conn;
    
    return conn->evcon[conn->next_evcon % ASYNC_PER_HOST_CONNECTION_LIMIT];
}

struct AsyncCallbackGroup *new_async_callback_group(struct evhttp_request *req, 
                                void (*finished_cb)(struct evhttp_request *, void *), void *finished_cb_arg)
{
    struct AsyncCallbackGroup *callback_group = NULL;
    
    callback_group = malloc(sizeof(*callback_group));
    callback_group->count = 0;
    callback_group->id = simplehttp_request_id(req);
    callback_group->original_request = req;
    callback_group->evb = evbuffer_new();
    callback_group->finished_cb = finished_cb;
    callback_group->finished_cb_arg = finished_cb_arg;
    TAILQ_INIT(&callback_group->callback_list);
    
    return callback_group;
}

void free_async_callback_group(struct AsyncCallbackGroup *callback_group)
{
    if (TAILQ_EMPTY(&callback_group->callback_list)) {
        if (callback_group->finished_cb) {
            callback_group->finished_cb(callback_group->original_request, callback_group->finished_cb_arg);
        }
        evbuffer_free(callback_group->evb);
        free(callback_group);
    }
}

void new_async_callback(struct AsyncCallbackGroup *callback_group, char *address, int port, char *path, 
                                void (*cb)(struct evhttp_request *, void *), void *cb_arg)
{
    /* create new connection to endpoint */
    struct AsyncCallback *callback = NULL;
    simplehttp_ts start_ts;
    
    simplehttp_ts_get(&start_ts);
    
    callback = malloc(sizeof(*callback));
    callback->start_ts = start_ts;
    callback->id = callback_group->count++;
    callback->callback_group = callback_group;
    callback->cb = cb;
    callback->cb_arg = cb_arg;
    
    AS_DEBUG("getting connection to %s:%d\n", address, port);
    
    callback->evcon = get_connection(address, port, &callback->conn);
    
    callback->request = evhttp_request_new(finish_async_request, callback);
    evhttp_add_header(callback->request->output_headers, "Host", address);
    
    AS_DEBUG("calling evhttp_make_request to %s\n", path);
    
    if (evhttp_make_request(callback->evcon, callback->request, EVHTTP_REQ_GET, path) == -1) {
        AS_DEBUG("REQUEST FAILED for source %s:%d%s\n", address, port, path);
        
        if (request_logging) {
            char host_buf[256];
            char id_buf[256];
            sprintf(host_buf, "%s:%d", address, port);
            sprintf(id_buf, "%"PRIu64":%"PRIu64, callback_group->id, callback->id);
            simplehttp_log(host_buf, callback->request, 0, id_buf);
        }
        
        // run this callback
        if (callback->cb) {
            // TODO: SHOULD THIS BE PASSED NULL since this didn't actually execute?
            callback->cb(callback->request, callback->cb_arg);
        }
        
        // dealloc the callback?
        free(callback);
    } else {
        TAILQ_INSERT_TAIL(&callback_group->callback_list, callback, entries);
    }
}

void finish_async_request(struct evhttp_request *req, void *cb_arg)
{
    struct AsyncCallback *callback = (struct AsyncCallback *)cb_arg;
    struct AsyncCallbackGroup *callback_group = callback->callback_group;
    simplehttp_ts end_ts;
    uint64_t req_time;
    
    simplehttp_ts_get(&end_ts);
    req_time = simplehttp_ts_diff(callback->start_ts, end_ts);
    
    if (request_logging) {
        char host_buf[256];
        char id_buf[256];
        sprintf(host_buf, "%s:%d", callback->conn->address, callback->conn->port);
        sprintf(id_buf, "%"PRIu64":%"PRIu64, callback_group->id, callback->id);
        simplehttp_log(host_buf, req, req_time, id_buf);
    }
    
#ifdef ASYNC_DEBUG
    char *temp_body = malloc(EVBUFFER_LENGTH(req->input_buffer)+1);
    memcpy(temp_body, EVBUFFER_DATA(req->input_buffer), EVBUFFER_LENGTH(req->input_buffer));
    temp_body[EVBUFFER_LENGTH(req->input_buffer)] = '\0';
    AS_DEBUG("HTTP %d %s\n", req->response_code, req->uri);
    AS_DEBUG("RESPONSE BODY %s\n", temp_body);
    free(temp_body);
#endif
    
    // run this callback
    if (callback->cb) {
        callback->cb(req, callback->cb_arg);
    }
    
    // remove from the list of callbacks
    TAILQ_REMOVE(&callback_group->callback_list, callback, entries);
    // free this object
    free(callback);
    
    // re-check if this callback_group needs to be freed
    free_async_callback_group(callback_group);
}
