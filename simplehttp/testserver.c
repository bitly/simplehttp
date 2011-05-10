#include <stdio.h>
#include "<siplehttp/simplehttp.h>"

#define VERSION "0.1"

void
cb(struct evhttp_request *req, struct evbuffer *evb,void *ctx)
{
    evbuffer_add_printf(evb, "Hello World!\n%s\n", req->uri);
    evhttp_send_reply(req, HTTP_OK, "OK", evb);
}

int version_cb(int value) {
    fprintf(stdout, "Version: %s\n", VERSION);
    return 0;
}


int
main(int argc, char **argv)
{
    define_simplehttp_options();
    option_define_bool("version", OPT_OPTIONAL, 0, NULL, version_cb, VERSION);
    
    if (!option_parse_command_line(argc, argv)){
        return 1;
    }
    
    simplehttp_init();
    simplehttp_set_cb("/ass*", cb, NULL);
    simplehttp_set_cb("/foo*", cb, NULL);
    simplehttp_set_cb("/bar*", cb, NULL);
    simplehttp_main();
    free_options();
    return 0;
}
