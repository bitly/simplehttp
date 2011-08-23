#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <event.h>
#include <evhttp.h>
#include <signal.h>
#include "pubsubclient.h"
#include "stream_request.h"

#ifdef DEBUG
#define _DEBUG(...) fprintf(stdout, __VA_ARGS__)
#else
#define _DEBUG(...) do {;} while (0)
#endif

static int chunked = 0;
static struct GlobalData *data = NULL;
static struct StreamRequest *stream_request = NULL;
static struct evhttp_connection *evhttp_source_connection = NULL;
static struct evhttp_request *evhttp_source_request = NULL;
static struct StreamRequest *autodetect_sr = NULL;
extern struct event_base *current_base;

struct GlobalData {
    void (*message_cb)(char *data, void *cbarg);
    void (*error_cb)(int status_code, void *cbarg);
    void *cbarg;
    const char *source_address;
    int source_port;
    const char *path;
};

void pubsubclient_termination_handler(int signum)
{
    event_loopbreak();
}

int pubsubclient_parse_one_message(struct evbuffer *evb, void *arg)
{
    char *line;
    size_t line_len;
    struct GlobalData *client_data;
    char *data;
    int has_more = 0;
    
    _DEBUG("pubsubclient_parse_one_message()\n");
    
    client_data = (struct GlobalData *)arg;
    line = evbuffer_readline(evb);
    if (line && (line_len = strlen(line))) {
        _DEBUG("line (%p): %s (%d)\n", line, line, line_len);
        (*client_data->message_cb)(line, client_data->cbarg);
        if (EVBUFFER_LENGTH(evb)) {
            has_more = 1;
        }
    }
    free(line);
    return has_more;
}

void pubsubclient_source_readcb(struct bufferevent *bev, void *arg)
{
    while (pubsubclient_parse_one_message(EVBUFFER_INPUT(bev), arg)) {}
}

void pubsubclient_errorcb(struct bufferevent *bev, void *arg)
{
    struct GlobalData *client_data = (struct GlobalData *)arg;
    
    fprintf(stderr, "ERROR: request failed\n");
    
    if (client_data->error_cb) {
        (*client_data->error_cb)(-1, client_data->cbarg);
    }
}

void pubsubclient_source_request_done(struct evhttp_request *req, void *arg)
{
    struct GlobalData *client_data = (struct GlobalData *)arg;
    int status_code = -1;
    
    _DEBUG("pubsubclient_source_request_done()\n");
    
    fprintf(stderr, "ERROR: request failed\n");
    
    if (req) {
        status_code = req->response_code;
    }
    
    if (client_data->error_cb) {
        (*client_data->error_cb)(status_code, client_data->cbarg);
    }
}

void pubsubclient_source_callback(struct evhttp_request *req, void *arg)
{
    // keep parsing out messages until there aren't any left
    while (pubsubclient_parse_one_message(req->input_buffer, arg)) {}
}

int pubsubclient_connect()
{
    fprintf(stdout, "CONNECTING TO SOURCE http://%s:%d%s\n", data->source_address, data->source_port, data->path);
    if (chunked) {
        if (evhttp_source_connection) {
            evhttp_connection_free(evhttp_source_connection);
        }
        
        // use libevent's built in evhttp methods to parse chunked responses
        evhttp_source_connection = evhttp_connection_new(data->source_address, data->source_port);
        if (evhttp_source_connection == NULL) {
            fprintf(stderr, "ERROR: evhttp_connection_new() failed for %s:%d\n", data->source_address, data->source_port);
            return 0;
        }
        
        evhttp_source_request = evhttp_request_new(pubsubclient_source_request_done, data);
        evhttp_add_header(evhttp_source_request->output_headers, "Host", data->source_address);
        evhttp_request_set_chunked_cb(evhttp_source_request, pubsubclient_source_callback);
        
        if (evhttp_make_request(evhttp_source_connection, evhttp_source_request, EVHTTP_REQ_GET, data->path) == -1) {
            fprintf(stderr, "ERROR: evhttp_make_request() failed for %s\n", data->path);
            evhttp_connection_free(evhttp_source_connection);
            return 0;
        }
    } else {
        if (stream_request) {
            free_stream_request(stream_request);
        }
        
        // use our stream_request library to handle non-chunked
        stream_request = new_stream_request("GET", data->source_address, data->source_port, data->path, 
                                NULL, pubsubclient_source_readcb, pubsubclient_errorcb, data);
        if (!stream_request) {
            fprintf(stderr, "ERROR: new_stream_request() failed for %s:%d%s\n", data->source_address, data->source_port, data->path);
            return 0;
        }
    }
    
    return 1;
}

void pubsubclient_autodetect_headercb(struct bufferevent *bev, struct evkeyvalq *headers, void *arg)
{
    const char *encoding_header = NULL;
    
    _DEBUG("pubsubclient_autodetect_headercb() headers: %p\n", headers);
    
    if ((encoding_header = evhttp_find_header(headers, "Transfer-Encoding")) != NULL) {
        if (strncmp(encoding_header, "chunked", 7) == 0) {
            chunked = 1;
        }
    }
    
    // turn off the events for this buffer
    // its free'd later
    bufferevent_disable(bev, EV_READ);
    
    _DEBUG("chunked = %d\n", chunked);
    
    if (!pubsubclient_connect(arg)) {
        event_loopbreak();
    }
}

void pubsubclient_init(const char *source_address, int source_port, const char *path, 
    void (*message_cb)(char *data, void *arg),
    void (*error_cb)(int status_code, void *arg),
    void *cbarg)
{
    
    signal(SIGINT, pubsubclient_termination_handler);
    signal(SIGQUIT, pubsubclient_termination_handler);
    signal(SIGTERM, pubsubclient_termination_handler);
    signal(SIGHUP, pubsubclient_termination_handler);
    
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
    
    // perform a request for headers so we can autodetect whether or not we're
    // getting a chunked response
    fprintf(stdout, "AUTODETECTING ENCODING FOR http://%s:%d%s\n", source_address, source_port, path);
    autodetect_sr = new_stream_request("HEAD", source_address, source_port, path, 
                        pubsubclient_autodetect_headercb, NULL, pubsubclient_errorcb, data);
    if (!autodetect_sr) {
        fprintf(stderr, "ERROR: new_stream_request() failed for %s:%d%s\n", source_address, source_port, path);
        exit(1);
    }
}

int pubsubclient_main(const char *source_address, int source_port, const char *path, 
    void (*message_cb)(char *data, void *arg),
    void (*error_cb)(int status_code, void *arg),
    void *cbarg)
{
    pubsubclient_init(source_address, source_port, path, message_cb, error_cb, cbarg);
    event_dispatch();
    pubsubclient_free();
    return 0;
}

void pubsubclient_run()
{
    event_dispatch();
}    

void pubsubclient_free()
{
    free_stream_request(autodetect_sr);
    
    if (chunked) {
        evhttp_connection_free(evhttp_source_connection);
    } else {
        free_stream_request(stream_request);
    }
    
    free(data);
}
