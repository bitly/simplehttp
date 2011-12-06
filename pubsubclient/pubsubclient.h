#ifndef __pubsubclient_h
#define __pubsubclient_h

#include <event.h>

struct StreamRequest;

int pubsubclient_main(const char *source_address, int source_port, const char *path,
                      void (*message_cb)(char *data, void *arg),
                      void (*error_cb)(int status_code, void *arg),
                      void *cbarg);
void pubsubclient_init(const char *source_address, int source_port, const char *path,
                       void (*message_cb)(char *data, void *arg),
                       void (*error_cb)(int status_code, void *arg),
                       void *cbarg);
int pubsubclient_connect();
void pubsubclient_run();
void pubsubclient_free();

struct StreamRequest *new_stream_request(const char *method, const char *source_address, int source_port, const char *path,
        void (*header_cb)(struct bufferevent *bev, struct evkeyvalq *headers, void *arg),
        void (*read_cb)(struct bufferevent *bev, void *arg),
        void (*error_cb)(struct bufferevent *bev, void *arg),
        void *arg);
void free_stream_request(struct StreamRequest *sr);

#endif
