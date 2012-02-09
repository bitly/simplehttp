#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <signal.h>
#include <event.h>
#include "buffered_socket.h"

// buffered_socket demo app
//
// $ nc -l 5150 (in another window)
// $ make
// $ ./demo

static struct BufferedSocket *buffsock;
static struct event reconnect_ev;
static struct event pipe_ev;

static void connect(int sig, short what, void *arg)
{
    buffered_socket_connect(buffsock);
    fprintf(stdout, "%s: attempting to connect\n", __FUNCTION__);
}

static void set_reconnect_timer()
{
    struct timeval tv = { 5, 0 };
    
    evtimer_del(&reconnect_ev);
    evtimer_set(&reconnect_ev, connect, NULL);
    evtimer_add(&reconnect_ev, &tv);
}

static void termination_handler(int signum)
{
    event_loopbreak();
}

static void ignore_cb(int sig, short what, void *arg)
{
    signal_set(&pipe_ev, SIGPIPE, ignore_cb, NULL);
    signal_add(&pipe_ev, NULL);
}

static void connect_cb(struct BufferedSocket *buffsock, void *arg)
{
    fprintf(stdout, "%s: connected to %s:%d\n", __FUNCTION__, buffsock->address, buffsock->port);
    buffered_socket_write(buffsock, arg, strlen(arg));
}

static void close_cb(struct BufferedSocket *buffsock, void *arg)
{
    fprintf(stdout, "%s: connection closed to %s:%d\n", __FUNCTION__, buffsock->address, buffsock->port);
    set_reconnect_timer();
}

static void read_cb(struct BufferedSocket *buffsock, uint8_t *data, size_t len, void *arg)
{
    fprintf(stdout, "%s: read %lu bytes\n", __FUNCTION__, len);
    fwrite(data, len, 1, stdout);
}

static void write_cb(struct BufferedSocket *buffsock, void *arg)
{
    // normally dont have to do anything here
}

static void error_cb(struct BufferedSocket *buffsock, void *arg)
{
    // track errors, make reconnect decisions
}

int main(int argc, char **argv)
{
    event_init();
    
    buffsock = new_buffered_socket("127.0.0.1", 5150,
        connect_cb, close_cb, read_cb, write_cb, error_cb, "hello world\n");
    
    signal(SIGINT, termination_handler);
    signal(SIGQUIT, termination_handler);
    signal(SIGTERM, termination_handler);
    
    signal_set(&pipe_ev, SIGPIPE, ignore_cb, NULL);
    signal_add(&pipe_ev, NULL);
    
    set_reconnect_timer();
    
    event_dispatch();
    
    free_buffered_socket(buffsock);
    
    return 0;
}
