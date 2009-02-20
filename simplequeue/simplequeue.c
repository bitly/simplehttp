#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "queue.h"
#include "simplehttp.h"

struct queue_entry {
	char *data;
	TAILQ_ENTRY(queue_entry) entries;
};

TAILQ_HEAD(, queue_entry) queues;

int depth = 0;
int depth_high_water = 0;
int n_puts = 0;
int n_gets = 0;

void
stats(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct evkeyvalq args;
    const char *reset;
    char *uri;
    
    uri = evhttp_decode_uri(req->uri);
    evhttp_parse_query(uri, &args);
    free(uri);
    reset = evhttp_find_header(&args, "reset");    
    if (reset != NULL && strcmp(reset, "1") == 0) {
        depth_high_water = 0;
        n_puts = 0;
        n_gets = 0;
    } else {
        evbuffer_add_printf(evb, "puts:%d\n", n_puts);
        evbuffer_add_printf(evb, "gets:%d\n", n_gets);
        evbuffer_add_printf(evb, "depth:%d\n", depth);
        evbuffer_add_printf(evb, "depth_high_water:%d", depth_high_water);
    }
    
    evhttp_send_reply(req, HTTP_OK, "OK", evb);
}   

void
get(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct queue_entry *entry;
    
    n_gets++;
    entry = TAILQ_FIRST(&queues);
    if (entry != NULL) {
        evbuffer_add_printf(evb, "%s", entry->data);
        TAILQ_REMOVE(&queues, entry, entries);
        free(entry->data);
        free(entry);
        depth--;
    }
    
    evhttp_send_reply(req, HTTP_OK, "OK", evb);
}

void
put(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct evkeyvalq args;
    struct queue_entry *entry;
    const char *data;
    char *uri;
    
    n_puts++;
    uri = evhttp_decode_uri(req->uri);
    evhttp_parse_query(uri, &args);
    free(uri);
    data = evhttp_find_header(&args, "data");
    entry = malloc(sizeof(*entry));
    entry->data = malloc(strlen(data)+1);
    strcpy(entry->data, data);
    TAILQ_INSERT_TAIL(&queues, entry, entries);
    depth++;
    if (depth > depth_high_water) {
        depth_high_water = depth;
    }
    
    evhttp_send_reply(req, HTTP_OK, "OK", evb);
}

void
dump(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct queue_entry *entry;

    TAILQ_FOREACH(entry, &queues, entries) {
        evbuffer_add_printf(evb, "%s\n", entry->data);
    }
    
    evhttp_send_reply(req, HTTP_OK, "OK", evb);
}

int
main(int argc, char **argv)
{
    TAILQ_INIT(&queues);
    
    simplehttp_init();
    simplehttp_set_cb("/put*", put, NULL);
    simplehttp_set_cb("/get*", get, NULL);
    simplehttp_set_cb("/dump*", dump, NULL);
    simplehttp_set_cb("/stats*", stats, NULL);
    simplehttp_main(argc, argv);
    
    return 0;
}