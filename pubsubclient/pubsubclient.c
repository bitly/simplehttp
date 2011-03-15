#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <simplehttp/queue.h>
#include <simplehttp/simplehttp.h>
#include "pubsubclient.h"

#define DEBUG 0
#define BOUNDARY_LENGTH 14
// \r\n--xXPubSubXx\r\n

int source_callback_helper(struct evhttp_request *req, void *arg);

struct global_data {
    void (*cb)(char *data, void *cbarg);
    void *cbarg;
};

void 
http_post_done(struct evhttp_request *req, void *arg){
    if (req->response_code != HTTP_OK) {
        fprintf(stderr, "FAILED post != OK\n");
        exit(1);
    }
    if (DEBUG) fprintf(stdout, "post_done OK\n");
}


void 
source_callback (struct evhttp_request *req, void *arg){
    while (source_callback_helper(req, arg) == 1){
        int i = 1;
    }
}

int 
source_callback_helper(struct evhttp_request *req, void *arg){
    if ((size_t)BOUNDARY_LENGTH > EVBUFFER_LENGTH(req->input_buffer)){
        return 0;
    }
    if (DEBUG) fprintf(stdout, "source_callback\n");
    // enum message_read_status done;
    evhttp_clear_headers(req->input_headers);
    evhttp_parse_headers(req, req->input_buffer);
    char *content_len;
    content_len = (char *) evhttp_find_header(req->input_headers, "Content-Length");
    if (!content_len){return 0; // drain buffer? // exit?
        }
    
    int len = atoi(content_len);
    size_t len_size;
    len_size = (size_t)len;
    if (DEBUG) fprintf(stdout, "received content_length:%d buffer has:%d\n", (int)len, (int)EVBUFFER_LENGTH(req->input_buffer));

    struct global_data *client_data = (struct global_data *)arg;

    char *data = calloc(len, sizeof(char *));
    evbuffer_remove(req->input_buffer, data, len);
    if (DEBUG)fprintf(stdout, "data has %d bytes\n", (int)strlen(data));
    if (DEBUG)fprintf(stdout, "data=%s\n", data);
    (*client_data->cb)(data, client_data->cbarg);
    free(data);
    // empty buffer
    // --
    if ((size_t)BOUNDARY_LENGTH <= EVBUFFER_LENGTH(req->input_buffer) ){
        evbuffer_drain(req->input_buffer, (size_t)BOUNDARY_LENGTH);
    }
    return 1;
}

void
http_chunked_request_done(struct evhttp_request *req, void *arg)
{
    if (DEBUG) fprintf(stdout, "http_chunked_request_done\n");
    if (req->response_code != HTTP_OK) {
        fprintf(stderr, "FAILED on sub connection\n");
        exit(1);
    }
    if (DEBUG) fprintf(stderr, "DONE on sub connection\n");
}


int
pubsub_to_pubsub_main(char *source_address, int source_port, void (*cb)(char *data, void *arg), void *cbarg)
{

    event_init();

    struct evhttp_connection *evhttp_source_connection = NULL;
    struct evhttp_request *evhttp_source_request = NULL;
    
    fprintf(stdout, "connecting to http://%s:%d/sub\n", source_address, source_port);
    evhttp_source_connection = evhttp_connection_new(source_address, source_port);
    if (evhttp_source_connection == NULL) {
        fprintf(stdout, "FAILED CONNECT TO SOURCE %s:%d\n", source_address, source_port);
        exit(1);
    }

    struct global_data *data;
    data = calloc(1, sizeof(*data));
    data->cb = cb;
    data->cbarg = cbarg;

    evhttp_source_request = evhttp_request_new(http_chunked_request_done, data);
    evhttp_add_header(evhttp_source_request->output_headers, "Host", source_address);
    evhttp_request_set_chunked_cb(evhttp_source_request, source_callback);

    if (evhttp_make_request(evhttp_source_connection, evhttp_source_request, EVHTTP_REQ_GET, "/sub") == -1) {
        fprintf(stdout, "FAILED make_request to source\n");
        evhttp_connection_free(evhttp_source_connection);
        return 1;
    }

    event_dispatch();
    evhttp_connection_free(evhttp_source_connection);
    fprintf(stdout, "EXITING\n");
    return 0;
}
