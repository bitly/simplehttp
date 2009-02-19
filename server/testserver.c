#include <stdio.h>
#include "simplehttp.h"

void
cb(struct evhttp_request *req, void *ctx)
{
    printf("my callback fired\n");
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
