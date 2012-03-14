#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netdb.h>
#include <event.h>
#include "buffered_socket.h"

#ifdef DEBUG
#define _DEBUG(...) fprintf(stdout, __VA_ARGS__)
#else
#define _DEBUG(...) do {;} while (0)
#endif

static void buffered_socket_readcb(struct bufferevent *bev, void *arg);
static void buffered_socket_writecb(struct bufferevent *bev, void *arg);
static void buffered_socket_errorcb(struct bufferevent *bev, short what, void *arg);
static void buffered_socket_connectcb(int fd, short what, void *arg);

struct BufferedSocket *new_buffered_socket(const char *address, int port, 
    void (*connect_callback)(struct BufferedSocket *buffsock, void *arg), 
    void (*close_callback)(struct BufferedSocket *buffsock, void *arg), 
    void (*read_callback)(struct BufferedSocket *buffsock, struct evbuffer *evb, void *arg), 
    void (*write_callback)(struct BufferedSocket *buffsock, void *arg), 
    void (*error_callback)(struct BufferedSocket *buffsock, void *arg),
    void *cbarg)
{
    struct BufferedSocket *buffsock;
    
    buffsock = malloc(sizeof(struct BufferedSocket));
    buffsock->address = strdup(address);
    buffsock->port = port;
    buffsock->bev = NULL;
    buffsock->fd = -1;
    buffsock->state = BS_INIT;
    buffsock->connect_callback = connect_callback;
    buffsock->close_callback = close_callback;
    buffsock->read_callback = read_callback;
    buffsock->write_callback = write_callback;
    buffsock->error_callback = error_callback;
    buffsock->cbarg = cbarg;
    
    return buffsock;
}

void free_buffered_socket(struct BufferedSocket *buffsock)
{
    if (buffsock) {
        buffered_socket_close(buffsock);
        free(buffsock->address);
        free(buffsock);
    }
}

int buffered_socket_connect(struct BufferedSocket *buffsock)
{
    struct addrinfo ai, *aitop;
    char strport[32];
    struct sockaddr *sa;
    int slen;
    
    if ((buffsock->state == BS_CONNECTED) || (buffsock->state == BS_CONNECTING)) {
        return 0;
    }
    
    memset(&ai, 0, sizeof(struct addrinfo));
    ai.ai_family = AF_INET;
    ai.ai_socktype = SOCK_STREAM;
    snprintf(strport, sizeof(strport), "%d", buffsock->port);
    if (getaddrinfo(buffsock->address, strport, &ai, &aitop) != 0) {
        _DEBUG("%s: getaddrinfo() failed\n", __FUNCTION__);
        return -1;
    }
    sa = aitop->ai_addr;
    slen = aitop->ai_addrlen;
    
    if ((buffsock->fd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        _DEBUG("%s: socket() failed\n", __FUNCTION__);
        return -1;
    }
    
    if (evutil_make_socket_nonblocking(buffsock->fd) == -1) {
        close(buffsock->fd);
        _DEBUG("%s: evutil_make_socket_nonblocking() failed\n");
        return -1;
    }
    
    if (connect(buffsock->fd, sa, slen) == -1) {
        if (errno != EINPROGRESS) {
            close(buffsock->fd);
            _DEBUG("%s: connect() failed\n");
            return -1;
        }
    }
    
    freeaddrinfo(aitop);
    
    struct timeval tv = { 2, 0 };
    event_set(&buffsock->conn_ev, buffsock->fd, EV_WRITE, buffered_socket_connectcb, buffsock);
    event_add(&buffsock->conn_ev, &tv);
    
    buffsock->state = BS_CONNECTING;
    
    return buffsock->fd;
}

static void buffered_socket_connectcb(int fd, short what, void *arg)
{
    struct BufferedSocket *buffsock = (struct BufferedSocket *)arg;
    int error;
    socklen_t errsz = sizeof(error);
    
    if (what == EV_TIMEOUT) {
        _DEBUG("%s: connection timeout for \"%s:%d\" on %d\n",
               __FUNCTION__, buffsock->address, buffsock->port, buffsock->fd);
        buffered_socket_close(buffsock);
        return;
    }
    
    if (getsockopt(buffsock->fd, SOL_SOCKET, SO_ERROR, (void*)&error, &errsz) == -1) {
        _DEBUG("%s: getsockopt failed for \"%s:%d\" on %d\n",
               __FUNCTION__, buffsock->address, buffsock->port, buffsock->fd);
        buffered_socket_close(buffsock);
        return;
    }
    
    if (error) {
        _DEBUG("%s: \"%s\" for \"%s:%d\" on %d\n",
               __FUNCTION__, strerror(error), buffsock->address, buffsock->port, buffsock->fd);
        buffered_socket_close(buffsock);
        return;
    }
    
    _DEBUG("%s: connected to \"%s:%d\" on %d\n", 
           __FUNCTION__, buffsock->address, buffsock->port, buffsock->fd);
    
    buffsock->state = BS_CONNECTED;
    buffsock->bev = bufferevent_new(buffsock->fd, 
        buffered_socket_readcb, buffered_socket_writecb, buffered_socket_errorcb, 
        (void *)buffsock);
    bufferevent_enable(buffsock->bev, EV_READ);
    
    if (buffsock->connect_callback) {
        (*buffsock->connect_callback)(buffsock, buffsock->cbarg);
    }
}

void buffered_socket_close(struct BufferedSocket *buffsock)
{
    _DEBUG("%s: closing \"%s:%d\" on %d\n", 
           __FUNCTION__, buffsock->address, buffsock->port, buffsock->fd);
    
    buffsock->state = BS_DISCONNECTED;
    
    if (event_initialized(&buffsock->conn_ev)) {
        event_del(&buffsock->conn_ev);
    }
    
    if (buffsock->fd != -1) {
        if (buffsock->close_callback) {
            (*buffsock->close_callback)(buffsock, buffsock->cbarg);
        }
        close(buffsock->fd);
        buffsock->fd = -1;
    }
    
    if (buffsock->bev) {
        bufferevent_free(buffsock->bev);
        buffsock->bev = NULL;
    }
}

size_t buffered_socket_write(struct BufferedSocket *buffsock, void *data, size_t len)
{
    if (buffsock->state != BS_CONNECTED) {
        return -1;
    }
    
    _DEBUG("%s: writing %lu bytes starting at %p\n", __FUNCTION__, len, data);
    
    bufferevent_write(buffsock->bev, data, len);
    bufferevent_enable(buffsock->bev, EV_WRITE);
    
    return len;
}

void buffered_socket_readcb(struct bufferevent *bev, void *arg)
{
    struct BufferedSocket *buffsock = (struct BufferedSocket *)arg;
    struct evbuffer *evb;
    
    _DEBUG("%s: %lu bytes read\n", __FUNCTION__, len);
    
    // client's responsibility to drain the buffer
    evb = EVBUFFER_INPUT(bev);
    if (buffsock->read_callback) {
        (*buffsock->read_callback)(buffsock, evb, buffsock->cbarg);
    }
}

void buffered_socket_writecb(struct bufferevent *bev, void *arg)
{
    struct BufferedSocket *buffsock = (struct BufferedSocket *)arg;
    struct evbuffer *evb;
    
    evb = EVBUFFER_OUTPUT(bev);
    if (EVBUFFER_LENGTH(evb) == 0) {
        bufferevent_disable(bev, EV_WRITE);
    }
    
    _DEBUG("%s: left to write %lu\n", __FUNCTION__, EVBUFFER_LENGTH(evb));
    
    if (buffsock->write_callback) {
        (*buffsock->write_callback)(buffsock, buffsock->cbarg);
    }
}

void buffered_socket_errorcb(struct bufferevent *bev, short what, void *arg)
{
    struct BufferedSocket *buffsock = (struct BufferedSocket *)arg;
    
    _DEBUG("%s\n", __FUNCTION__);
    
    if (buffsock->error_callback) {
        (*buffsock->error_callback)(buffsock, buffsock->cbarg);
    }
    
    buffered_socket_close(buffsock);
}
