#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <sys/stat.h>
#include <signal.h>
#include <event.h>
#include "domain_socket.h"

/*
domain_socket demo app

$ python -c 'import socket; \
s = socket.socket(socket.AF_UNIX); \
s.connect("/tmp/domain_socket_test"); \
s.send("hi\n"); \
o = s.recv(4096); \
print o'
*/

static struct DomainSocket *uds;
static struct event pipe_ev;

static void termination_handler(int signum)
{
    event_loopbreak();
}

static void ignore_cb(int sig, short what, void *arg)
{
    signal_set(&pipe_ev, SIGPIPE, ignore_cb, NULL);
    signal_add(&pipe_ev, NULL);
}

static void uds_on_read(struct DSClient *client)
{
    struct bufferevent *bev = client->bev;
    struct evbuffer *evb;
    char *cmd;
    
    while ((cmd = evbuffer_readline(bev->input)) != NULL) {
        if (strcmp(cmd, "hi") == 0) {
            evb = evbuffer_new();
            evbuffer_add_printf(evb, "hello world\n");
            domain_socket_client_write(client, EVBUFFER_DATA(evb), EVBUFFER_LENGTH(evb));
            evbuffer_free(evb);
        }
        free(cmd);
    }
}

static void uds_on_write(struct DSClient *client)
{}

static void uds_on_error(struct DSClient *client)
{}

int main(int argc, char **argv)
{
    event_init();
    
    if (!(uds = new_domain_socket("/tmp/domain_socket_test", 
            S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH, 
            uds_on_read, uds_on_write, uds_on_error, 64))) {
        fprintf(stdout, "ERROR: new_domain_socket() failed\n");
        exit(1);
    }
    
    signal(SIGINT, termination_handler);
    signal(SIGQUIT, termination_handler);
    signal(SIGTERM, termination_handler);
    
    signal_set(&pipe_ev, SIGPIPE, ignore_cb, NULL);
    signal_add(&pipe_ev, NULL);
    
    event_dispatch();
    
    free_domain_socket(uds);
    
    return 0;
}
