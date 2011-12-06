#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <netdb.h>
#include <errno.h>
#include <event.h>
#include <evhttp.h>
#include "stream_request.h"

#ifdef DEBUG
#define _DEBUG(...) fprintf(stdout, __VA_ARGS__)
#else
#define _DEBUG(...) do {;} while (0)
#endif

enum read_states { read_firstline, read_headers, read_body };

struct StreamRequest *new_stream_request(const char *method, const char *source_address, int source_port, const char *path,
        void (*header_cb)(struct bufferevent *bev, struct evkeyvalq *headers, void *arg),
        void (*read_cb)(struct bufferevent *bev, void *arg),
        void (*error_cb)(struct bufferevent *bev, void *arg),
        void *arg) {
    struct StreamRequest *sr;
    int fd;
    struct evbuffer *http_request;
    
    fd = stream_request_connect(source_address, source_port);
    if (fd == -1) {
        return NULL;
    }
    
    sr = malloc(sizeof(struct StreamRequest));
    sr->fd = fd;
    sr->state = read_firstline;
    sr->header_cb = header_cb;
    sr->read_cb = read_cb;
    sr->error_cb = error_cb;
    sr->arg = arg;
    sr->bev = bufferevent_new(sr->fd, stream_request_readcb, stream_request_writecb, stream_request_errorcb, sr);
    http_request = evbuffer_new();
    evbuffer_add_printf(http_request, "%s %s HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", method, path, source_address);
    bufferevent_write(sr->bev, (char *)EVBUFFER_DATA(http_request), EVBUFFER_LENGTH(http_request));
    evbuffer_free(http_request);
    
    return sr;
}

void free_stream_request(struct StreamRequest *sr)
{
    if (sr) {
        stream_request_disconnect(sr->fd);
        bufferevent_free(sr->bev);
        free(sr);
    }
}

int stream_request_connect(const char *address, int port)
{
    struct addrinfo ai, *aitop;
    char strport[NI_MAXSERV];
    struct sockaddr *sa;
    int slen;
    int fd;
    
    memset(&ai, 0, sizeof(struct addrinfo));
    ai.ai_family = AF_INET;
    ai.ai_socktype = SOCK_STREAM;
    snprintf(strport, sizeof(strport), "%d", port);
    if (getaddrinfo(address, strport, &ai, &aitop) != 0) {
        fprintf(stderr, "ERROR: getaddrinfo() failed\n");
        return -1;
    }
    sa = aitop->ai_addr;
    slen = aitop->ai_addrlen;
    
    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd == -1) {
        fprintf(stderr, "ERROR: socket() failed\n");
        return -1;
    }
    
    if (fcntl(fd, F_SETFL, O_NONBLOCK) == -1) {
        fprintf(stderr, "ERROR: fcntl(O_NONBLOCK) failed\n");
        return -1;
    }
    
    if (connect(fd, sa, slen) == -1) {
        if (errno != EINPROGRESS) {
            fprintf(stderr, "ERROR: connect() failed\n");
            return -1;
        }
    }
    
    freeaddrinfo(aitop);
    
    return fd;
}

int stream_request_disconnect(int fd)
{
    EVUTIL_CLOSESOCKET(fd);
}

void stream_request_readcb(struct bufferevent *bev, void *arg)
{
    struct evhttp_request *req;
    struct StreamRequest *sr = (struct StreamRequest *)arg;
    
    _DEBUG("stream_request_readcb()\n");
    
    switch (sr->state) {
        case read_firstline:
            req = evhttp_request_new(NULL, NULL);
            req->kind = EVHTTP_RESPONSE;
            // 1 is the constant ALL_DATA_READ in http-internal.h
            if (evhttp_parse_firstline(req, EVBUFFER_INPUT(bev)) == 1) {
                sr->state = read_headers;
            }
            evhttp_request_free(req);
            // dont break, try to parse the headers too
        case read_headers:
            req = evhttp_request_new(NULL, NULL);
            req->kind = EVHTTP_RESPONSE;
            // 1 is the constant ALL_DATA_READ in http-internal.h
            if (evhttp_parse_headers(req, EVBUFFER_INPUT(bev)) == 1) {
                if (sr->header_cb) {
                    sr->header_cb(sr->bev, req->input_headers, sr->arg);
                }
                sr->state = read_body;
            }
            evhttp_request_free(req);
            break;
        case read_body:
            if (sr->read_cb) {
                sr->read_cb(sr->bev, sr->arg);
            }
            break;
    }
}

void stream_request_writecb(struct bufferevent *bev, void *arg)
{
    _DEBUG("stream_request_writecb()\n");
    
    if (EVBUFFER_LENGTH(EVBUFFER_OUTPUT(bev)) == 0) {
        bufferevent_enable(bev, EV_READ);
    }
}

void stream_request_errorcb(struct bufferevent *bev, short what, void *arg)
{
    struct StreamRequest *sr = (struct StreamRequest *)arg;
    
    _DEBUG("stream_request_errorcb()\n");
    
    if (sr->error_cb) {
        sr->error_cb(sr->bev, sr->arg);
    }
}
