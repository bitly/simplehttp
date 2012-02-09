#ifndef __domain_socket_h
#define __domain_socket_h

struct DSClient;

struct DomainSocket {
    int fd;
    char *path;
    void (*read_callback)(struct DSClient *client);
    void (*write_callback)(struct DSClient *client);
    void (*error_callback)(struct DSClient *client);
    struct event ev;
};

struct DSClient {
    int fd;
    struct bufferevent *bev;
    struct DomainSocket *uds;
};

struct DomainSocket *new_domain_socket(const char *path, int access_mask, 
    void (*read_callback)(struct DSClient *client), 
    void (*write_callback)(struct DSClient *client), 
    void (*error_callback)(struct DSClient *client), 
    int listen_backlog);
void free_domain_socket(struct DomainSocket *uds);
void domain_socket_client_write(struct DSClient *client, void *data, size_t len);

#endif
