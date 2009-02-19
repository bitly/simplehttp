#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "queue.h"
#include "simplehttp.h"

struct queue_entry {
	char *message;
	TAILQ_ENTRY(queue_entry) entries;
};

TAILQ_HEAD(, queue_entry) queues;

int count = 0;

void
put(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct evkeyvalq args;
    struct queue_entry *entry;
    const char *message;
    
    evhttp_parse_query(req->uri, &args);
    message = evhttp_find_header(&args, "m");
    entry = malloc(sizeof(*entry));
    entry->message = malloc(strlen(message)+1);
    strcpy(entry->message, message);
    TAILQ_INSERT_TAIL(&queues, entry, entries);
    count++;
    
    evhttp_send_reply(req, HTTP_OK, "OK", evb);
}

void
dump(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct queue_entry *entry;

    TAILQ_FOREACH(entry, &queues, entries) {
        fprintf(stderr, "%s\n", entry->message);
    }
}

int
main(int argc, char **argv)
{
    TAILQ_INIT(&queues);
    
    simplehttp_init();
    simplehttp_set_cb("/put*", put, NULL);
    simplehttp_set_cb("/dump*", dump, NULL);
    simplehttp_main(argc, argv);
    
    return 0;
}