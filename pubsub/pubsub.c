#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "queue.h"
#include "simplehttp.h"

#define BUFSZ 1024
#define BOUNDARY "xXPubSubXx"

typedef struct cli {
    struct evbuffer *buf;
    struct evhttp_request *req;
    TAILQ_ENTRY(cli) entries;
} cli;
TAILQ_HEAD(, cli) clients;

uint32_t totalConns = 0;
uint32_t currentConns = 0;
uint32_t msgRecv = 0;
uint32_t msgSent = 0;

void
stats_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct evkeyvalq args;
    char *reset, *uri;
    char buf[33];
    
    uri = evhttp_decode_uri(req->uri);
    evhttp_parse_query(uri, &args);
    free(uri);
    
    sprintf(buf, "%ld", totalConns);
    evhttp_add_header(req->output_headers, "X-PUBSUB-TOTAL-CONNECTIONS", buf);
    sprintf(buf, "%ld", currentConns);
    evhttp_add_header(req->output_headers, "X-PUBSUB-ACTIVE-CONNECTIONS", buf);
    sprintf(buf, "%ld", msgRecv);
    evhttp_add_header(req->output_headers, "X-PUBSUB-MESSAGES-RECEIVED", buf);
    sprintf(buf, "%ld", msgSent);
    evhttp_add_header(req->output_headers, "X-PUBSUB-MESSAGES-SENT", buf);
    
    evbuffer_add_printf(evb, "Active connections: %ld\nTotal connections: %ld\n"
                             "Messages received: %ld\nMessages sent: %ld\n",
                             currentConns, totalConns, msgRecv, msgSent); 
    reset = (char *)evhttp_find_header(&args, "reset");

    if (reset) {
        totalConns = 0;
        currentConns = 0;
        msgRecv = 0;
        msgSent = 0;
    } 

    evhttp_send_reply(req, HTTP_OK, "OK", evb);
    evhttp_clear_headers(&args);
}

void on_close(struct evhttp_connection *evcon, void *ctx)
{
    struct cli *client = (struct cli *)ctx;

    if (client) {
        currentConns--;
        TAILQ_REMOVE(&clients, client, entries);
        evbuffer_free(client->buf);
        free(client);
    }
}

void pub_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    int i = 0;
    struct cli *client;
    
    msgRecv++;
    totalConns++;
  
    TAILQ_FOREACH(client, &clients, entries) {
        msgSent++;
        evbuffer_drain(client->buf, EVBUFFER_LENGTH(client->buf));
        
        evbuffer_add_printf(client->buf, 
                            "content-type: %s\r\ncontent-length: %d\r\n\r\n",
                            "*/*",
                            EVBUFFER_LENGTH(req->input_buffer));
        evbuffer_add(client->buf, req->input_buffer->buffer, EVBUFFER_LENGTH(req->input_buffer));
        evbuffer_add_printf(client->buf, "\r\n--%s\r\n", BOUNDARY);
        
        evhttp_send_reply_chunk(client->req, client->buf);
        i++;
    }
    
    evbuffer_add_printf(evb, "Published to %d clients.\n", i);
    evhttp_send_reply(req, HTTP_OK, "OK", evb);
}

void sub_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct cli *client;

    currentConns++;
    totalConns++;
    client = calloc(1, sizeof(*client));
    client->req = req;
    client->buf = evbuffer_new();
    evhttp_add_header(client->req->output_headers, "content-type",
        "multipart/x-mixed-replace; boundary=" BOUNDARY);
    evbuffer_add_printf(client->buf, "--%s\r\n", BOUNDARY);
    evhttp_send_reply_start(client->req, HTTP_OK, "OK");
    evhttp_send_reply_chunk(client->req, client->buf);
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
    simplehttp_set_cb("/sta*", stats_cb, NULL);
    simplehttp_main(argc, argv);

    return 0;
}
