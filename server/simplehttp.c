#include <sys/types.h>
#include <sys/time.h>
#include <sys/stat.h>

#include <signal.h>
#include <stdlib.h>
#include <unistd.h>
#include <pwd.h>
#include <grp.h>
#include <err.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <fnmatch.h>

#include "queue.h"
#include "simplehttp.h"


static void
ignore_cb(int sig, short what, void *arg)
{
}

void
termination_handler (int signum)
{
    event_loopbreak();
}

void
generic_request_handler(struct evhttp_request *req, void *arg)
{
    int found_cb = 0;
    struct cb_entry *entry;
    struct evbuffer *evb = evbuffer_new();
    
    if (debug) {
        fprintf(stderr, "request for %s from %s\n", req->uri, req->remote_host);
    }

    TAILQ_FOREACH(entry, &callbacks, entries) {
        if (fnmatch(entry->path, req->uri, FNM_NOESCAPE) == 0) {
            (*entry->cb)(req, evb, entry->ctx);
            found_cb = 1;
            break;
        }
    }

    if (!found_cb) {
        evhttp_send_reply(req, HTTP_NOTFOUND, "", evb);
    }
    evbuffer_free(evb);
}


int main(int argc, char **argv)
{
    char *address = "0.0.0.0";
    int port = 1957;
    int ch;
    struct evhttp *httpd;
    struct event pipe_ev;
    
    opterr = 0;
    while ((ch = getopt(argc, argv, "a:p:d:")) != -1) {
        switch (ch) {
        case 'a':
            address = optarg;
            break;
        case 'p':
            port = atoi(optarg);
            break;
        case 'd':
            debug = 1;
            break;
        }
    }
    
    signal(SIGINT, termination_handler);
    signal(SIGQUIT, termination_handler);
    signal(SIGTERM, termination_handler);

    event_init();

    signal_set(&pipe_ev, SIGPIPE, ignore_cb, NULL);
    signal_add(&pipe_ev, NULL);

    httpd = evhttp_start(address, port);
    if (!httpd) {
        printf("could not bind to %s:%d\n", address, port);
        exit(1);
    }

    printf("listening on %s:%d\n", address, port);

    evhttp_set_gencb(httpd, generic_request_handler, NULL);
    event_dispatch();

    printf("exiting\n");
    /* Not reached in this code as it is now. */
    evhttp_free(httpd);

    return 0;
}
