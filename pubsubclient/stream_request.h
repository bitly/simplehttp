#ifndef __stream_request_h
#define __stream_request_h

struct StreamRequest {
    int fd;
    struct bufferevent *bev;
    void *arg;
    int state;
    void (*header_cb)(struct bufferevent *bev, struct evkeyvalq *headers, void *arg);
    void (*read_cb)(struct bufferevent *bev, void *arg);
    void (*error_cb)(struct bufferevent *bev, void *arg);
};

int stream_request_connect(const char *address, int port);
int stream_request_disconnect(int fd);
void stream_request_readcb(struct bufferevent *bev, void *arg);
void stream_request_writecb(struct bufferevent *bev, void *arg);
void stream_request_errorcb(struct bufferevent *bev, short what, void *arg);

#endif
