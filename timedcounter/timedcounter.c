#include "queue.h"
#include <stdlib.h>
#include <stdio.h>
#include "simplehttp.h"
#include "tcl.h"

typedef struct decrement {
    int                 amt;
    Tcl_HashEntry       *hPtr;
    struct event        ev;
} decrement;


Tcl_HashTable ht;

static void
argtoi(struct evkeyvalq *args, const char *key, int *val, int def)
{
    const char *tmp;

    *val = def;
    tmp = evhttp_find_header(args, key);
    if (tmp) {
        *val = atoi(tmp);
    }
}

void
decrtime(int fd, short what, void *ctx)
{
    struct decrement *decr = (struct decrement *) ctx;
    int i;

    i = (int)Tcl_GetHashValue(decr->hPtr);
    i -= decr->amt;
    Tcl_SetHashValue(decr->hPtr, (void *)i);
    evtimer_del(&decr->ev);
    free(decr);
}

void
cb(struct evhttp_request *req, struct evbuffer *evb,void *ctx)
{
    const char          *k;
    int                 incr, i, new;
    struct timeval      tv = {0,0};
    struct evkeyvalq    args;
    struct decrement    *decr;
    Tcl_HashEntry       *hPtr;

    evhttp_parse_query(req->uri, &args);
    
    k = evhttp_find_header(&args, "key");
#define DEFAULT_DECAY 60
    argtoi(&args, "duration", (int *) &(tv.tv_sec), DEFAULT_DECAY);
    argtoi(&args, "increment", &incr, 1);

    if (k == NULL) {
        evhttp_send_error(req, 400, "key required: ?key=");
        evhttp_clear_headers(&args);
        return;
    } else if (incr < 0) {
        evhttp_send_error(req, 400, "incr must be >= 0");
        evhttp_clear_headers(&args);
        return;
    }

    hPtr = Tcl_CreateHashEntry(&ht, k, &new);
    if (new) {
        Tcl_SetHashValue(hPtr, incr);
        i = incr;
    } else {
        i = (int)Tcl_GetHashValue(hPtr);
        i += incr;
        Tcl_SetHashValue(hPtr, i);
    }

    decr = malloc(sizeof(*decr));
    decr->hPtr = hPtr;
    decr->amt  = incr;

    evtimer_set(&decr->ev, decrtime, decr);
    evtimer_add(&decr->ev, &tv);

    evbuffer_add_printf(evb, "%d\n", i);
    evhttp_send_reply(req, HTTP_OK, "OK", evb);
    evhttp_clear_headers(&args);
}

int
main(int argc, char **argv)
{
    Tcl_InitHashTable(&ht, TCL_STRING_KEYS);

    simplehttp_init();
    simplehttp_set_cb("/*", cb, NULL);
    simplehttp_main(argc, argv);

    return 0;
}
