#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <simplehttp/queue.h>
#include <simplehttp/simplehttp.h>
#include <signal.h>
#include "pubsubclient.h"

#ifdef DEBUG
#define _DEBUG(...) fprintf(stdout, __VA_ARGS__)
#else
#define _DEBUG(...) do {;} while (0)
#endif

struct global_data {
    void (*cb)(char *data, void *cbarg);
    void *cbarg;
};

void pubsubclient_termination_handler(int signum)
{
    event_loopbreak();
}

int parse_one_message(struct evhttp_request *req, void *arg)
{
    char *line;
    size_t line_len;
    struct global_data *client_data;
    char *data;
    
    _DEBUG("parse_one_message()\n");
    
    client_data = (struct global_data *)arg;
    line = evbuffer_readline(req->input_buffer);
    line_len = line ? strlen(line) : 0;
    if (line && line_len) {
        evbuffer_drain(req->input_buffer, line_len);
        _DEBUG("line (%p): %s (%d)\n", line, line, line_len);
        (*client_data->cb)(line, client_data->cbarg);
        if (EVBUFFER_LENGTH(req->input_buffer)) {
            free(line);
            return 1;
        }
    }
    free(line);
    return 0;
}

void source_callback(struct evhttp_request *req, void *arg)
{
    // keep parsing out messages until there aren't any left
    while (parse_one_message(req, arg)) {}
}

void http_source_request_done(struct evhttp_request *req, void *arg)
{
    _DEBUG("http_chunked_request_done\n");
    if (!req || req->response_code != HTTP_OK) {
        fprintf(stderr, "FAILED on sub connection\n");
        event_loopbreak();
    }
    _DEBUG("DONE on sub connection\n");
}

int pubsub_to_pubsub_main(const char *source_address, int source_port, const char *path, void (*cb)(char *data, void *arg), void *cbarg)
{
    struct evhttp_connection *evhttp_source_connection = NULL;
    struct evhttp_request *evhttp_source_request = NULL;
    struct global_data *data;
    
    signal(SIGINT, pubsubclient_termination_handler);
    signal(SIGQUIT, pubsubclient_termination_handler);
    signal(SIGTERM, pubsubclient_termination_handler);
    
    event_init();
    
    fprintf(stdout, "CONNECTING TO http://%s:%d%s\n", source_address, source_port, path);
    evhttp_source_connection = evhttp_connection_new(source_address, source_port);
    if (evhttp_source_connection == NULL) {
        fprintf(stdout, "FAILED CONNECT TO SOURCE %s:%d\n", source_address, source_port);
        exit(1);
    }
    
    data = calloc(1, sizeof(struct global_data));
    data->cb = cb;
    data->cbarg = cbarg;
    
    evhttp_source_request = evhttp_request_new(http_source_request_done, data);
    evhttp_add_header(evhttp_source_request->output_headers, "Host", source_address);
    evhttp_request_set_chunked_cb(evhttp_source_request, source_callback);
    
    if (evhttp_make_request(evhttp_source_connection, evhttp_source_request, EVHTTP_REQ_GET, path) == -1) {
        fprintf(stdout, "FAILED make_request to source\n");
        evhttp_connection_free(evhttp_source_connection);
        return 1;
    }
    
    event_dispatch();
    evhttp_connection_free(evhttp_source_connection);
    free(data);
    
    return 0;
}
