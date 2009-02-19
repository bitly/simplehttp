#include <stdio.h>
#include "simplehttp.h"

void
cb(struct evhttp_request *req, struct evbuffer *evb,void *ctx)
{
    evbuffer_add_printf(evb, "Hello bitches\n%s\n", req->uri);
    evhttp_send_reply(req, HTTP_OK, "OK", evb);
}

int
main(int argc, char **argv)
{
    simplehttp_init();
    simplehttp_set_cb("/ass*", cb, NULL);
    simplehttp_set_cb("/foo*", cb, NULL);
    simplehttp_set_cb("/bar*", cb, NULL);
    simplehttp_main(argc, argv);
    return 0;
}
