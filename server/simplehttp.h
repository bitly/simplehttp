#include <event.h>
#include <evhttp.h>
#include "queue.h"

void simplehttp_init();
int simplehttp_main(int argc, char **argv);
void simplehttp_set_cb(char *path, void *cb, void *ctx);

