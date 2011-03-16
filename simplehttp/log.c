#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include "simplehttp.h"

const char *simplehttp_method(struct evhttp_request *req)
{
    const char *method;
    
    switch (req->type) {
        case EVHTTP_REQ_GET:
            method = "GET";
            break;
        case EVHTTP_REQ_POST:
            method = "POST";
            break;
        case EVHTTP_REQ_HEAD:
            method = "HEAD";
            break;
        default:
            method = NULL;
            break;
    }
    
    return method;
}

void simplehttp_log(const char *host, struct evhttp_request *req, uint64_t req_time, const char *id)
{
    // NOTE: this is localtime not gmtime
    time_t now;
    struct tm *tm_now;
    char datetime_buf[64];
    char type;
    
    time(&now);
    tm_now = localtime(&now);
    strftime(datetime_buf, 64, "%y%m%d %H:%M:%S", tm_now);
    
    if (req->response_code >= 500 && req->response_code < 600) {
        type = 'E';
    } else if (req->response_code >= 400 && req->response_code < 500) {
        type = 'W';
    } else {
        type = 'I';
    }
    
    fprintf(stdout, "[%c %s %s] %d %s %s%s", type, datetime_buf, id, req->response_code, simplehttp_method(req), host, req->uri);
    
    if (req->type == EVHTTP_REQ_POST) {
        fprintf(stdout, "?");
        fwrite(EVBUFFER_DATA(req->input_buffer), EVBUFFER_LENGTH(req->input_buffer), 1, stdout);
    }
    
    fprintf(stdout, " %.3fms\n", req_time / 1000.0);
}
