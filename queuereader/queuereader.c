#define _GNU_SOURCE // for strndup()
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <event.h>
#include <evhttp.h>
#include <signal.h>
#include <math.h>
#include <simplehttp/simplehttp.h>
#include "queuereader.h"

#ifdef DEBUG
#define _DEBUG(...) fprintf(stdout, __VA_ARGS__)
#else
#define _DEBUG(...) do {;} while (0)
#endif

#define MAX_BACKOFF_COUNTER 8

void queuereader_termination_handler(int signum);
void queuereader_trigger_source_request(int seconds, int microseconds);
void queuereader_free();
void queuereader_source_request(int fd, short what, void *cbarg);
void queuereader_source_cb(struct evhttp_request *req, void *cbarg);
void queuereader_requeue_message_request();
void queuereader_requeue_message_cb(struct evhttp_request *req, void *cbarg);
int queuereader_calculate_backoff_seconds();
void queuereader_increment_backoff();
void queuereader_decrement_backoff();

static char *message = NULL;
static int backoff_counter = 0;
static struct GlobalData *data = NULL;
static struct event ev;
static int sleeptime_queue_empty_ms = 500;
static struct timeval sleeptime_queue_empty_tv;
extern struct event_base *current_base;

struct GlobalData {
    int (*message_cb)(char *data, void *cbarg);
    void (*error_cb)(int status_code, void *cbarg);
    void *cbarg;
    const char *source_address;
    int source_port;
    const char *path;
};

void queuereader_termination_handler(int signum)
{
    event_loopbreak();
}

int queuereader_calculate_backoff_seconds()
{
    int seconds;
    
    seconds = backoff_counter ? (1 << backoff_counter) : 0;
    if (seconds > 0) {
        fprintf(stderr, "NOTICE: backing off for %d seconds\n", seconds);
    }
    
    return seconds;
}

void queuereader_trigger_source_request(int seconds, int microseconds)
{
    struct timeval tv = { seconds, microseconds };
    
    evtimer_del(&ev);
    evtimer_set(&ev, queuereader_source_request, data);
    evtimer_add(&ev, &tv);
}

void queuereader_decrement_backoff()
{
    // bounded decrement of backoff counter
    backoff_counter = ((backoff_counter - 1) > 0) ? (backoff_counter - 1) : 0;
}

void queuereader_increment_backoff()
{
    // bounded increment of backoff counter
    backoff_counter = ((backoff_counter + 1) > MAX_BACKOFF_COUNTER) ?
                      MAX_BACKOFF_COUNTER : (backoff_counter + 1);
}

void queuereader_finish_message(int return_code)
{
    switch (return_code) {
        case QR_CONT:
            // do nothing
            break;
        case QR_EMPTY:
            queuereader_trigger_source_request(sleeptime_queue_empty_tv.tv_sec, sleeptime_queue_empty_tv.tv_usec);
            break;
        case QR_SUCCESS:
            queuereader_decrement_backoff();
            queuereader_trigger_source_request(queuereader_calculate_backoff_seconds(), 0);
            break;
        case QR_FAILURE:
            queuereader_increment_backoff();
            queuereader_requeue_message_request();
            break;
        case QR_CONT_SOURCE_REQUEST:
            queuereader_trigger_source_request(queuereader_calculate_backoff_seconds(), 0);
            break;
        case QR_REQUEUE_WITHOUT_BACKOFF:
            queuereader_requeue_message_request();
            break;
    }
}

void queuereader_requeue_message_cb(struct evhttp_request *req, void *cbarg)
{
    // TODO: this should limit (and perhaps backoff) failed requeue requests
    queuereader_finish_message((req && req->response_code == 200) ?
                               QR_CONT_SOURCE_REQUEST : QR_REQUEUE_WITHOUT_BACKOFF);
}

void queuereader_requeue_message_request()
{
    struct evbuffer *evb;
    char *encoded_message;
    
    fprintf(stderr, "NOTICE: requeue message %s\n", message);
    evb = evbuffer_new();
    encoded_message = simplehttp_encode_uri(message);
    evbuffer_add_printf(evb, "/put?data=%s", encoded_message);
    new_async_request((char *)data->source_address, data->source_port,
                      (char *)EVBUFFER_DATA(evb), queuereader_requeue_message_cb, (void *)NULL);
    evbuffer_free(evb);
    free(encoded_message);
}

void queuereader_source_cb(struct evhttp_request *req, void *cbarg)
{
    struct GlobalData *client_data = (struct GlobalData *)cbarg;
    char *line;
    size_t line_len;
    struct evbuffer *evb;
    int ret = QR_EMPTY;
    
    if (message) {
        free(message);
        message = NULL;
    }
    
    evb = req->input_buffer;
    line = (char *)EVBUFFER_DATA(evb);
    line_len = EVBUFFER_LENGTH(evb);
    if (line_len) {
        message = strndup(line, line_len);
        ret = (*client_data->message_cb)(message, client_data->cbarg);
    }
    
    queuereader_finish_message(ret);
}

void queuereader_source_request(int fd, short what, void *cbarg)
{
    struct GlobalData *client_data = (struct GlobalData *)cbarg;
    struct evbuffer *evb;
    
    evb = evbuffer_new();
    evbuffer_add_printf(evb, client_data->path);
    new_async_request((char *)client_data->source_address, client_data->source_port,
                      (char *)EVBUFFER_DATA(evb), queuereader_source_cb, cbarg);
    evbuffer_free(evb);
}

void queuereader_set_sleeptime_queue_empty_ms(int milliseconds)
{
    sleeptime_queue_empty_ms = milliseconds;
}

void queuereader_init(const char *source_address, int source_port, const char *path,
                      int (*message_cb)(char *data, void *arg),
                      void (*error_cb)(int status_code, void *arg),
                      void *cbarg)
{
    int timeout_seconds = floor((sleeptime_queue_empty_ms * 1000) / 1000000);
    int timeout_microseconds = (sleeptime_queue_empty_ms * 1000) - (timeout_seconds * 1000000);
    
    sleeptime_queue_empty_tv.tv_sec = timeout_seconds;
    sleeptime_queue_empty_tv.tv_usec = timeout_microseconds;
    
    signal(SIGINT, queuereader_termination_handler);
    signal(SIGQUIT, queuereader_termination_handler);
    signal(SIGTERM, queuereader_termination_handler);
    signal(SIGHUP, queuereader_termination_handler);
    
    if (!current_base) {
        event_init();
    }
    
    data = calloc(1, sizeof(struct GlobalData));
    data->message_cb = message_cb;
    data->error_cb = error_cb;
    data->cbarg = cbarg;
    data->source_address = source_address;
    data->source_port = source_port;
    data->path = path;
    
    queuereader_trigger_source_request(sleeptime_queue_empty_tv.tv_sec, sleeptime_queue_empty_tv.tv_usec);
}

int queuereader_main(const char *source_address, int source_port, const char *path,
                     int (*message_cb)(char *data, void *arg),
                     void (*error_cb)(int status_code, void *arg),
                     void *cbarg)
{
    queuereader_init(source_address, source_port, path, message_cb, error_cb, cbarg);
    queuereader_run();
    queuereader_free();
    return 0;
}

void queuereader_run()
{
    event_dispatch();
}

void queuereader_free()
{
    free(data);
}
