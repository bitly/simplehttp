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

typedef struct cb_entry {
    char *path;
    void (*cb)(struct evhttp_request *, struct evbuffer *,void *);
    void *ctx;
    TAILQ_ENTRY(cb_entry) entries;
} cb_entry;
TAILQ_HEAD(, cb_entry) callbacks;

int debug = 0;

static void
ignore_cb(int sig, short what, void *arg)
{
}

void
termination_handler (int signum)
{
    event_loopbreak();
}

int 
get_uid(char *user)
{
    int retcode;
    struct passwd *pw;

    pw = getpwnam(user);
    if (pw == NULL) {
        retcode = -1;
    } else {
        retcode = (unsigned) pw->pw_uid;
    }
    return retcode;
}

int 
get_gid(char *group)
{
    int retcode;
    struct group *grent;

    grent = getgrnam(group);
    if (grent == NULL) {
        retcode = -1;
    } else {
        retcode = grent->gr_gid;
    }
    return retcode;
}

int 
get_user_gid(char *user)
{
    int retcode;
    struct passwd *pw;

    pw = getpwnam(user);
    if (pw == NULL) {
        retcode = -1;
    } else {
        retcode = pw->pw_gid;
    }
    return retcode;
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

void
simplehttp_init()
{
    event_init();
    TAILQ_INIT(&callbacks);
}

void
simplehttp_set_cb(char *path, void (*cb)(struct evhttp_request *, struct evbuffer *, void *), void *ctx)
{
    struct cb_entry *cbPtr;

    cbPtr = (cb_entry *)malloc(sizeof(*cbPtr));
    cbPtr->path = strdup(path);
    cbPtr->cb = cb;
    cbPtr->ctx = ctx;
    TAILQ_INSERT_TAIL(&callbacks, cbPtr, entries);

    printf("registering callback for path \"%s\"\n", path);
}

int
simplehttp_main(int argc, char **argv)
{
    uid_t uid = 0;
    gid_t gid = 0;
    char *address;
    char *root = NULL;
    char *garg = NULL;
    char *uarg = NULL;
    int daemon = 0;
    int port, ch, errno;
    pid_t pid, sid;
    struct evhttp *httpd;
    struct event pipe_ev;
    
    address = "0.0.0.0";
    port = 8080;
    opterr = 0;
    while ((ch = getopt(argc, argv, "a:p:d:D:r:u:g:")) != -1) {
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
        case 'r':
            root = optarg;
            break;
        case 'D':
            daemon = 1;
            break;
        case 'g':
            garg = optarg;
            break;
        case 'u':
            uarg = optarg;
            break;
        }
    }
    
    if (daemon) {
        pid = fork();
        if (pid < 0) {
            exit(EXIT_FAILURE);
        } else if (pid > 0) {
            exit(EXIT_SUCCESS);
        }

        umask(0);
        sid = setsid();
        if (sid < 0) {
            exit(EXIT_FAILURE);
        }
    }
   
    if (uarg != NULL) {
        uid = get_uid(uarg);
        gid = get_user_gid(uarg);
        if (uid < 0) {
            uid = atoi(uarg);
            uarg = NULL;
        }
        if (uid == 0) {
            err(1, "invalid user");
        }
    }
    if (garg != NULL) {
        gid = get_gid(garg);
        if (gid < 0) {
            gid = atoi(garg);
            if (gid == 0) {
                err(1, "invalid group");
            }
        }
    }
   
    if (root != NULL) {
        if (chroot(root) != 0) {
            err(1, strerror(errno));
        }
        if (chdir("/") != 0) {
            err(1, strerror(errno));
        }
    }
    
    if (getuid() == 0) {
        if (uarg != NULL) {
            if (initgroups(uarg, (int) gid) != 0) {
                err(1, "initgroups() failed");
            }
        } else {
            if (setgroups(0, NULL) != 0) {
                err(1, "setgroups() failed");
            }
        }
        if (gid != getgid() && setgid(gid) != 0) {
            err(1, "setgid() failed");
        }
        if (setuid(uid) != 0) {
            err(1, "setuid() failed");
        }
    }
    
    signal(SIGINT, termination_handler);
    signal(SIGQUIT, termination_handler);
    signal(SIGTERM, termination_handler);

    signal_set(&pipe_ev, SIGPIPE, ignore_cb, NULL);
    signal_add(&pipe_ev, NULL);

    httpd = evhttp_start(address, port);
    if (!httpd) {
        printf("could not bind to %s:%d\n", address, port);
        return 1;
    }

    printf("listening on %s:%d\n", address, port);

    evhttp_set_gencb(httpd, generic_request_handler, NULL);
    event_dispatch();

    printf("exiting\n");
    /* Not reached in this code as it is now. */
    evhttp_free(httpd);

    return 0;
}
