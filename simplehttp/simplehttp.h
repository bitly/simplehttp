#ifndef _SIMPLEHTTP_H
#define _SIMPLEHTTP_H

#include "queue.h"
#include "options.h"
#include <event.h>
#include <evhttp.h>

#if _POSIX_TIMERS > 0

typedef struct timespec simplehttp_ts;

void simplehttp_ts_get(struct timespec *ts);
unsigned int simplehttp_ts_diff(struct timespec start, struct timespec end);

#else

typedef struct timeval simplehttp_ts;

void simplehttp_ts_get(struct timeval *ts);
unsigned int simplehttp_ts_diff(struct timeval start, struct timeval end);

#endif

struct simplehttp_stats {
    uint64_t requests;
    uint64_t *stats_counts;
    uint64_t *average_requests;
    uint64_t *ninety_five_percents;
    char **stats_labels;
    int callback_count;
};

void simplehttp_init();
int simplehttp_main();
void simplehttp_set_cb(char *path, void (*cb)(struct evhttp_request *, struct evbuffer *,void *), void *ctx);

uint64_t simplehttp_request_id(struct evhttp_request *req);
void simplehttp_async_enable(struct evhttp_request *req);
void simplehttp_async_finish(struct evhttp_request *req);

void simplehttp_log(const char *host, struct evhttp_request *req, uint64_t req_time, const char *id);

struct simplehttp_stats *simplehttp_stats_new();
void simplehttp_stats_get(struct simplehttp_stats *st);
void simplehttp_stats_free(struct simplehttp_stats *st);
uint64_t ninety_five_percent(int64_t *int_array, int length);
char **simplehttp_callback_names();

struct AsyncCallbackGroup;

/* start a new callback_group. memory will be freed after a call to 
    release_callback_group or when all the callbacks have been run */
struct AsyncCallbackGroup *new_async_callback_group(struct evhttp_request *req, void (*finished_cb)(struct evhttp_request *, void *), void *finished_cb_arg);
/* create a new AsyncCallback. delegation of memory for this callback 
    will be passed to callback_group */
int new_async_callback(struct AsyncCallbackGroup *callback_group, char *address, int port, char *path, void (*cb)(struct evhttp_request *, void *), void *cb_arg);
void free_async_callback_group(struct AsyncCallbackGroup *callback_group);
void init_async_connection_pool(int enable_request_logging);
void free_async_connection_pool();

enum response_formats {json_format, txt_format};
int get_argument_format(struct evkeyvalq *args);
int get_int_argument(struct evkeyvalq *args, char *key, int default_value);
double get_double_argument(struct evkeyvalq *args, char *key, double default_value);

void define_simplehttp_options();

#endif
