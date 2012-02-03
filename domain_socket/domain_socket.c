#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/stat.h>
#include <assert.h>
#include <event.h>
#include "domain_socket.h"

#ifdef DEBUG
#define _DEBUG(...) fprintf(stdout, __VA_ARGS__)
#else
#define _DEBUG(...) do {;} while (0)
#endif

static struct DSClient *new_domain_socket_client(struct DomainSocket *uds,
    int client_fd, struct sockaddr *sa, socklen_t salen);
static void free_domain_socket_client(struct DSClient *client);
static void accept_socket(int fd, short what, void *arg);

struct DomainSocket *new_domain_socket(const char *path, int access_mask, 
    void (*read_callback)(struct DSClient *client), 
    void (*write_callback)(struct DSClient *client), 
    void (*error_callback)(struct DSClient *client),
    int listen_backlog)
{
    struct linger ling = {0, 0};
    struct sockaddr_un addr;
    struct stat tstat;
    int flags = 1;
    int old_umask;
    struct DomainSocket *uds;
    
    assert(path != NULL);
    
    uds = malloc(sizeof(struct DomainSocket));
    uds->path = strdup(path);
    uds->fd = -1;
    uds->read_callback = read_callback;
    uds->write_callback = write_callback;
    uds->error_callback = error_callback;
    
    if ((uds->fd = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        _DEBUG("%s: socket() failed\n", __FUNCTION__);
        free_domain_socket(uds);
        return NULL;
    }
    
    // clean up a previous socket file if we left it around
    if (lstat(path, &tstat) == 0) { 
        if (S_ISSOCK(tstat.st_mode)) {
            unlink(path);
        }
    }
    
    // @jayridge doesn't think this does anything here
    setsockopt(uds->fd, SOL_SOCKET, SO_REUSEADDR, (void *)&flags, sizeof(flags));
    // @jayridge doesn't think this does anything here
    setsockopt(uds->fd, SOL_SOCKET, SO_KEEPALIVE, (void *)&flags, sizeof(flags));
    setsockopt(uds->fd, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling));
    
    // clears nonstandard fields in some impementations that otherwise mess things up
    memset(&addr, 0, sizeof(addr));
    
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, path, sizeof(addr.sun_path) - 1);
    assert(strcmp(addr.sun_path, path) == 0);
    old_umask = umask(~(access_mask & 0777));
    if (bind(uds->fd, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
        _DEBUG("%s: bind() failed\n", __FUNCTION__);
        free_domain_socket(uds);
        umask(old_umask);
        return NULL;
    }
    umask(old_umask);
    
    if (listen(uds->fd, listen_backlog) == -1) {
        _DEBUG("%s: listen() failed\n", __FUNCTION__);
        free_domain_socket(uds);
        return NULL;
    }
    
    if (evutil_make_socket_nonblocking(uds->fd) == -1) {
        _DEBUG("%s: evutil_make_socket_nonblocking() failed\n", __FUNCTION__);
        free_domain_socket(uds);
        return NULL;
    }
    
    event_set(&uds->ev, uds->fd, EV_READ | EV_PERSIST, accept_socket, uds);
    if (event_add(&uds->ev, NULL) == -1) {
        _DEBUG("%s: event_add() failed\n", __FUNCTION__);
        free_domain_socket(uds);
        return NULL;
    }
    
    return uds;
}

void free_domain_socket(struct DomainSocket *uds)
{
    struct stat tstat;
    
    if (uds) {
        event_del(&uds->ev);
        
        if (uds->fd != -1) {
            close(uds->fd);
        }
        
        if (lstat(uds->path, &tstat) == 0) { 
            if (S_ISSOCK(tstat.st_mode)) {
                unlink(uds->path);
            }
        }
        
        free(uds->path);
        free(uds);
    }
}

static void accept_socket(int fd, short what, void *arg)
{
    struct DomainSocket *uds = (struct DomainSocket *)arg;
    struct sockaddr_storage ss;
    socklen_t addrlen = sizeof(ss);
    int nfd;
    
    _DEBUG("%s: %d\n", __FUNCTION__, fd);
    
    if ((nfd = accept(fd, (struct sockaddr *)&ss, &addrlen)) == -1) {
        if (errno != EAGAIN && errno != EINTR) {
            _DEBUG("%s: bad accept", __FUNCTION__);
        }
        return;
    }
    
    if (evutil_make_socket_nonblocking(nfd) < 0) {
        return;
    }
    
    new_domain_socket_client(uds, nfd, (struct sockaddr *)&ss, addrlen);
}

// called by libevent when there is data to read.
static void buffered_on_read(struct bufferevent *bev, void *arg)
{
    struct DSClient *client = (struct DSClient *)arg;
    struct DomainSocket *uds = (struct DomainSocket *)client->uds;
    
    _DEBUG("%s: %d\n", __FUNCTION__, client->fd);
    
    if (*uds->read_callback) {
        (*uds->read_callback)(client);
    }
}

// called by libevent when the write buffer reaches 0.
static void buffered_on_write(struct bufferevent *bev, void *arg)
{
    struct DSClient *client = (struct DSClient *)arg;
    struct DomainSocket *uds = (struct DomainSocket *)client->uds;
    struct evbuffer *evb;
    
    _DEBUG("%s: %d\n", __FUNCTION__, client->fd);
    
    evb = EVBUFFER_OUTPUT(bev);
    if (EVBUFFER_LENGTH(evb) == 0) {
        bufferevent_disable(bev, EV_WRITE);
    }
    
    if (*uds->write_callback) {
        (*uds->write_callback)(client);
    }
}

// called by libevent when there is an error on the underlying socket descriptor.
static void buffered_on_error(struct bufferevent *bev, short what, void *arg)
{
    struct DSClient *client = (struct DSClient *)arg;
    struct DomainSocket *uds = (struct DomainSocket *)client->uds;
    
    _DEBUG("%s: client socket error, disconnecting\n", __FUNCTION__);
    
    if (*uds->error_callback) {
        (*uds->error_callback)(client);
    }
    
    free_domain_socket_client(client);
}

void domain_socket_client_write(struct DSClient *client, void *data, size_t len)
{
    bufferevent_write(client->bev, data, len);
    bufferevent_enable(client->bev, EV_WRITE);
}

static struct DSClient *new_domain_socket_client(struct DomainSocket *uds,
    int client_fd, struct sockaddr *sa, socklen_t salen)
{
    struct DSClient *client;
    
    client = malloc(sizeof(struct DSClient));
    client->uds = uds;
    client->fd = client_fd;
    client->bev = bufferevent_new(client_fd, buffered_on_read, buffered_on_write, buffered_on_error, client);
    bufferevent_enable(client->bev, EV_READ);
    
    _DEBUG("%s: %d\n", __FUNCTION__, client->fd);
    
    return client;
}

static void free_domain_socket_client(struct DSClient *client)
{
    if (client) {
        bufferevent_free(client->bev);
        if (client->fd != -1) {
            close(client->fd);
        }
        free(client);
    }
}
