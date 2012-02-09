#ifndef __buffered_socket_h
#define __buffered_socket_h

enum BufferedSocketStates {
    BS_INIT,
    BS_CONNECTING, 
    BS_CONNECTED, 
    BS_DISCONNECTED
};

struct BufferedSocket {
    char *address;
    int port;
    int fd;
    int state;
    struct event conn_ev;
    struct bufferevent *bev;
    void (*connect_callback)(struct BufferedSocket *buffsock, void *arg);
    void (*close_callback)(struct BufferedSocket *buffsock, void *arg);
    void (*read_callback)(struct BufferedSocket *buffsock, uint8_t *data, size_t len, void *arg);
    void (*write_callback)(struct BufferedSocket *buffsock, void *arg); 
    void (*error_callback)(struct BufferedSocket *buffsock, void *arg);
    void *cbarg;
};

struct BufferedSocket *new_buffered_socket(const char *address, int port, 
    void (*connect_callback)(struct BufferedSocket *buffsock, void *arg), 
    void (*close_callback)(struct BufferedSocket *buffsock, void *arg), 
    void (*read_callback)(struct BufferedSocket *buffsock, uint8_t *data, size_t len, void *arg), 
    void (*write_callback)(struct BufferedSocket *buffsock, void *arg), 
    void (*error_callback)(struct BufferedSocket *buffsock, void *arg),
    void *cbarg);
void free_buffered_socket(struct BufferedSocket *socket);
int buffered_socket_connect(struct BufferedSocket *buffsock);
void buffered_socket_close(struct BufferedSocket *socket);
size_t buffered_socket_write(struct BufferedSocket *buffsock, void *data, size_t len);

#endif
