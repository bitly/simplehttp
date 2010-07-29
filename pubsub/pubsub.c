#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "queue.h"
#include "simplehttp.h"

#define BUFSZ 1024
#define BOUNDARY "xXPubSubXx"

int ps_debug = 0;

typedef struct cli {
    int multipart;
    int websocket;
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
argtoi(struct evkeyvalq *args, char *key, int *val, int def)
{
    char *tmp;

    *val = def;
    tmp = (char *)evhttp_find_header(args, (const char *)key);
    if (tmp) {
        *val = atoi(tmp);
    }
}

void
clients_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct cli *client;
    
    TAILQ_FOREACH(client, &clients, entries) {
        evbuffer_add_printf(evb, "%s:%d\n", client->req->remote_host, client->req->remote_port);
    }
    
    evhttp_send_reply(req, HTTP_OK, "OK", evb);
}

void
stats_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct evkeyvalq args;
    char *reset, *uri;
    char buf[33];
    
    uri = evhttp_decode_uri(req->uri);
    evhttp_parse_query(uri, &args);
    free(uri);
    
    sprintf(buf, "%d", totalConns);
    evhttp_add_header(req->output_headers, "X-PUBSUB-TOTAL-CONNECTIONS", buf);
    sprintf(buf, "%d", currentConns);
    evhttp_add_header(req->output_headers, "X-PUBSUB-ACTIVE-CONNECTIONS", buf);
    sprintf(buf, "%d", msgRecv);
    evhttp_add_header(req->output_headers, "X-PUBSUB-MESSAGES-RECEIVED", buf);
    sprintf(buf, "%d", msgSent);
    evhttp_add_header(req->output_headers, "X-PUBSUB-MESSAGES-SENT", buf);
    
    evbuffer_add_printf(evb, "Active connections: %d\nTotal connections: %d\n"
                             "Messages received: %d\nMessages sent: %d\n",
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
        
        if (client->websocket) {
            // set to non-chunked so that send_reply_chunked doesn't add \r\n before/after this block
            client->req->chunked = 0;
            // write the frame. a websocket frame is \x00 + msg + \xFF
            evbuffer_add(client->buf, "\0", 1);
            evbuffer_add(client->buf, EVBUFFER_DATA(req->input_buffer), EVBUFFER_LENGTH(req->input_buffer));
            evbuffer_add(client->buf, "\xFF", 1);
        }
        else if (client->multipart) {
            /* chunked */
            evbuffer_add_printf(client->buf, 
                                "content-type: %s\r\ncontent-length: %d\r\n\r\n",
                                "*/*",
                                (int)EVBUFFER_LENGTH(req->input_buffer));
            evbuffer_add(client->buf, EVBUFFER_DATA(req->input_buffer), EVBUFFER_LENGTH(req->input_buffer));
            evbuffer_add_printf(client->buf, "\r\n--%s\r\n", BOUNDARY);
        } else {
            /* new line terminated */
            evbuffer_add(client->buf, EVBUFFER_DATA(req->input_buffer), EVBUFFER_LENGTH(req->input_buffer));
            evbuffer_add_printf(client->buf, "\n");
        }
        evhttp_send_reply_chunk(client->req, client->buf);
        i++;
    }
    
    evbuffer_add_printf(evb, "Published to %d clients.\n", i);
    evhttp_send_reply(req, HTTP_OK, "OK", evb);
}

void sub_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct cli *client;
    struct evkeyvalq args;
    char *uri;
    char *ws_origin;
    char *ws_upgrade;
    char *host;
    char buf[248];

    currentConns++;
    totalConns++;
    uri = evhttp_decode_uri(req->uri);
    evhttp_parse_query(uri, &args);
    free(uri);
    client = calloc(1, sizeof(*client));
    argtoi(&args, "multipart", &client->multipart, 1);
    client->req = req;
    client->buf = evbuffer_new();

    // Connection: Upgrade
    // Upgrade: WebSocket
    ws_upgrade = (char *) evhttp_find_header(req->input_headers, "Upgrade");
    ws_origin = (char *) evhttp_find_header(req->input_headers, "Origin");
    host = (char *) evhttp_find_header(req->input_headers, "Host");
    
    if (ps_debug && ws_upgrade) {
        fprintf(stderr, "upgrade header is %s\n", ws_upgrade);
        fprintf(stderr, "multipart is %d\n", client->multipart);
    }

    if (ws_upgrade && strstr(ws_upgrade, "WebSocket") != NULL) {
        if (ps_debug) {
            fprintf(stderr, "upgrading connection to a websocket\n");
        }
        client->websocket = 1;
        client->req->major = 1;
        client->req->minor = 1;
        evhttp_add_header(client->req->output_headers, "Upgrade", "WebSocket");
        evhttp_add_header(client->req->output_headers, "Connection", "Upgrade");
        evhttp_add_header(client->req->output_headers, "Server", "simplehttp/pubsub");
        if (ws_origin) {
            evhttp_add_header(client->req->output_headers, "WebSocket-Origin", ws_origin);
        }
        if (host) {
            sprintf(buf, "ws://%s%s", host, req->uri);
            if (ps_debug) {
                fprintf(stderr, "setting WebSocket-Location to %s\n", buf);
            }
            evhttp_add_header(client->req->output_headers, "WebSocket-Location", buf);
        }
        // evbuffer_add_printf(client->buf, "\r\n");
    }
    else if (client->multipart) {
        evhttp_add_header(client->req->output_headers, "content-type",
            "multipart/x-mixed-replace; boundary=" BOUNDARY);
        evbuffer_add_printf(client->buf, "--%s\r\n", BOUNDARY);
    } else {
        evhttp_add_header(client->req->output_headers, "content-type",
            "application/json");
        evbuffer_add_printf(client->buf, "\r\n");
    }
    if (client->websocket) {
        evhttp_send_reply_start(client->req, 101, "Web Socket Protocol Handshake");
    } else {
        evhttp_send_reply_start(client->req, HTTP_OK, "OK");
    }
    evhttp_send_reply_chunk(client->req, client->buf);
    TAILQ_INSERT_TAIL(&clients, client, entries);
    evhttp_connection_set_closecb(req->evcon, on_close, (void *)client);
    evhttp_clear_headers(&args);
}

int
main(int argc, char **argv)
{
    TAILQ_INIT(&clients);
    simplehttp_init();
    simplehttp_set_cb("/pub*", pub_cb, NULL);
    simplehttp_set_cb("/sub*", sub_cb, NULL);
    simplehttp_set_cb("/stats*", stats_cb, NULL);
    simplehttp_set_cb("/clients*", clients_cb, NULL);
    simplehttp_main(argc, argv);

    return 0;
}
