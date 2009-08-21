#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "simplehttp.h"
#include "queue.h"


typedef struct cli {
    struct evbuffer *buf;
    struct evhttp_request *req;
    TAILQ_ENTRY(cli) entries;
} cli;
TAILQ_HEAD(, cli) clients;


void on_close(struct evhttp_connection *evcon, void *ctx)
{
    struct cli *client = (struct cli *)ctx;

    if (client) {
        TAILQ_REMOVE(&clients, client, entries);
        evbuffer_free(client->buf);
        free(client);
    }
}

void pub_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    int i = 0;
    struct cli *client;
  
    TAILQ_FOREACH(client, &clients, entries) {
        evbuffer_drain(client->buf, EVBUFFER_LENGTH(client->buf));
        evbuffer_add(client->buf, req->input_buffer->buffer, EVBUFFER_LENGTH(req->input_buffer));
        evbuffer_add(client->buf, "\r\n", 2);
        printf("sending to client\n");
        evhttp_send_reply_chunk(client->req, client->buf);
        i++;
    }

    evbuffer_add_printf(evb, "Published to %d clients.\n", i);
    evhttp_send_reply(req, HTTP_OK, "OK", evb);
}

void sub_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct cli *client;

    printf("sub_cb\n");
    evhttp_send_reply_start(req, HTTP_OK, "OK");
 
    client = calloc(1, sizeof(*client));
    client->req = req;
    client->buf = evbuffer_new();
    TAILQ_INSERT_TAIL(&clients, client, entries);

    evhttp_connection_set_closecb(req->evcon, on_close, (void *)client);
}

int
main(int argc, char **argv)
{
    TAILQ_INIT(&clients);
    simplehttp_init();
    simplehttp_set_cb("/pub*", pub_cb, NULL);
    simplehttp_set_cb("/sub*", sub_cb, NULL);
    simplehttp_main(argc, argv);

    return 0;
}
