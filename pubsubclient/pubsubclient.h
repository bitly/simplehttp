#ifndef __pubsubclient_h
#define __pubsubclient_h

struct StreamRequest;

int pubsubclient_main(const char *source_address, int source_port, const char *path, void (*cb)(char *data, void *arg), void *cbarg);

struct StreamRequest *new_stream_request(const char *method, const char *source_address, int source_port, const char *path, 
    void (*header_cb)(struct bufferevent *bev, struct evkeyvalq *headers, void *arg), 
    void (*read_cb)(struct bufferevent *bev, void *arg), 
    void (*error_cb)(struct bufferevent *bev, void *arg), 
    void *arg);
void free_stream_request(struct StreamRequest *sr);

#endif
