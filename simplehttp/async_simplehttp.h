#ifndef _ASYNC_SIMPLEHTTP_H_
#define _ASYNC_SIMPLEHTTP_H_

#include <queue.h>
#include <simplehttp.h>

#ifdef ASYNC_DEBUG
#define AS_DEBUG(...) fprintf(stdout, __VA_ARGS__)
#else
#define AS_DEBUG(...) do {;} while (0)
#endif

#define ASYNC_PER_HOST_CONNECTION_LIMIT 10

struct AsyncCallbackGroup;
struct AsyncCallback;
struct Connection;

/* handling for callbacks */
struct AsyncCallback {
    simplehttp_ts start_ts;
    struct Connection *conn;
    struct evhttp_connection *evcon;
    struct evhttp_request *request;
    uint64_t id;
    void (*cb)(struct evhttp_request *req, void *);
    void *cb_arg;
    struct AsyncCallbackGroup *callback_group;
    TAILQ_ENTRY(AsyncCallback) entries;
};

struct AsyncCallbackGroup {
    struct evhttp_request *original_request;
    struct evbuffer *evb;
    uint64_t id;
    unsigned int count;
    time_t connect_time;
    void (*finished_cb)(struct evhttp_request *, void *);
    void *finished_cb_arg;
    TAILQ_HEAD(, AsyncCallback) callback_list;
};

struct Connection {
    struct evhttp_connection *evcon[ASYNC_PER_HOST_CONNECTION_LIMIT];
    uint64_t next_evcon;
    char *address;
    int port;
    TAILQ_ENTRY(Connection) next;
};

void finish_async_request(struct evhttp_request *req, void *cb_arg);
struct evhttp_connection *get_connection(char *address, int port, struct Connection **store_conn);


#endif
