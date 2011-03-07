#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include "simplehttp/queue.h"
#include "simplehttp/simplehttp.h"
#include "http-internal.h"

#define BOUNDARY "xXPubSubXx"
#define MAX_PENDING_DATA 1024*1024*50

int ps_debug = 0;

enum kick_client_enum {
    CLIENT_OK = 0,
    KICK_CLIENT = 1,
};
typedef struct cli {
    int multipart;
    int websocket;
    enum kick_client_enum kick_client;
    uint64_t connection_id;
    time_t connect_time;
    struct evbuffer *buf;
    struct evhttp_request *req;
    TAILQ_ENTRY(cli) entries;
} cli;
TAILQ_HEAD(, cli) clients;

uint64_t totalConns = 0;
uint64_t currentConns = 0;
uint64_t kickedClients = 0;
uint64_t msgRecv = 0;
uint64_t msgSent = 0;



int
is_slow(struct cli *client) {
    if (client->kick_client == KICK_CLIENT) { return 1; }
    struct evhttp_connection *evcon;
    unsigned long output_buffer_length;
    
    evcon = (struct evhttp_connection *)client->req->evcon;
    output_buffer_length = evcon->output_buffer ? (unsigned long)EVBUFFER_LENGTH(evcon->output_buffer) : 0;
    if (output_buffer_length > MAX_PENDING_DATA) {
        kickedClients+=1;
        fprintf(stdout, "%llu >> kicking client with %lu pending data\n", client->connection_id, output_buffer_length);
        client->kick_client = KICK_CLIENT;
        // clear the clients output buffer
        evbuffer_drain(evcon->output_buffer, EVBUFFER_LENGTH(evcon->output_buffer));
        evbuffer_add_printf(evcon->output_buffer, "ERROR_TOO_SLOW. kicked for having %lu pending bytes\n", output_buffer_length); 
        return 1;
    }
    return 0;
}

int 
can_kick(struct cli *client) {
    if (client->kick_client == CLIENT_OK){return 0;}
    // if the buffer length is back to zero, we can kick now
    // our error notice has been pushed to the client
    struct evhttp_connection *evcon;
    evcon = (struct evhttp_connection *)client->req->evcon;
    if (EVBUFFER_LENGTH(evcon->output_buffer) == 0){
        return 1;
    }
    return 0;
}

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
    struct tm *time_struct;
    char buf[248];
    unsigned long output_buffer_length;
    struct evhttp_connection *evcon;
    
    if (TAILQ_EMPTY(&clients)) {
        evbuffer_add_printf(evb, "no /sub connections\n");
    }
    TAILQ_FOREACH(client, &clients, entries) {
        evcon = (struct evhttp_connection *)client->req->evcon;
        
        time_struct = gmtime(&client->connect_time);
        strftime(buf, 248, "%Y-%m-%d %H:%M:%S", time_struct);
        output_buffer_length = (unsigned long)EVBUFFER_LENGTH(evcon->output_buffer);
        evbuffer_add_printf(evb, "%s:%d connected at %s. output buffer size:%lu state:%d\n", 
            client->req->remote_host, 
            client->req->remote_port, 
            buf, 
            output_buffer_length,
            (int)evcon->state);
    }
    
    evhttp_send_reply(req, HTTP_OK, "OK", evb);
}

void
stats_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct evkeyvalq args;
    char buf[33];
    const char *reset;
    const char *format;
    
    sprintf(buf, "%llu", totalConns);
    evhttp_add_header(req->output_headers, "X-PUBSUB-TOTAL-CONNECTIONS", buf);
    sprintf(buf, "%llu", currentConns);
    evhttp_add_header(req->output_headers, "X-PUBSUB-ACTIVE-CONNECTIONS", buf);
    sprintf(buf, "%llu", msgRecv);
    evhttp_add_header(req->output_headers, "X-PUBSUB-MESSAGES-RECEIVED", buf);
    sprintf(buf, "%llu", msgSent);
    evhttp_add_header(req->output_headers, "X-PUBSUB-MESSAGES-SENT", buf);
    sprintf(buf, "%llu", kickedClients);
    evhttp_add_header(req->output_headers, "X-PUBSUB-KICKED-CLIENTS", buf);
    
    evhttp_parse_query(req->uri, &args);
    format = (char *)evhttp_find_header(&args, "format");
    
    if ((format != NULL) && (strcmp(format, "json") == 0)) {
        evbuffer_add_printf(evb, "{");
        evbuffer_add_printf(evb, "\"current_connections\": %llu,", currentConns);
        evbuffer_add_printf(evb, "\"total_connections\": %llu,", totalConns);
        evbuffer_add_printf(evb, "\"messages_received\": %llu,", msgRecv);
        evbuffer_add_printf(evb, "\"messages_sent\": %llu,", msgSent);
        evbuffer_add_printf(evb, "\"kicked_clients\": %llu,", kickedClients);
        evbuffer_add_printf(evb, "}\n");
    } else {
        evbuffer_add_printf(evb, "Active connections: %llu\n", currentConns);
        evbuffer_add_printf(evb, "Total connections: %llu\n", totalConns);
        evbuffer_add_printf(evb, "Messages received: %llu\n", msgRecv);
        evbuffer_add_printf(evb, "Messages sent: %llu\n", msgSent);
        evbuffer_add_printf(evb, "Kicked clients: %llu\n", kickedClients);
    }
    
    reset = (char *)evhttp_find_header(&args, "reset");
    if (reset) {
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
        fprintf(stdout, "%llu >> close from  %s:%d\n", client->connection_id, evcon->address, evcon->port);
        currentConns--;
        TAILQ_REMOVE(&clients, client, entries);
        evbuffer_free(client->buf);
        free(client);
    } else {
        fprintf(stdout, "[unknown] >> close from  %s:%d\n", evcon->address, evcon->port);
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
        if (is_slow(client)) {
            if (can_kick(client)) {
                evhttp_connection_free(client->req->evcon);
                continue;
            }
            continue;
        }
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
    struct tm *time_struct;

    currentConns++;
    totalConns++;
    evhttp_parse_query(req->uri, &args);
    client = calloc(1, sizeof(*client));
    argtoi(&args, "multipart", &client->multipart, 1);
    client->req = req;
    client->connection_id = totalConns;
    client->connect_time = time(NULL);
    time_struct = gmtime(&client->connect_time);
    client->buf = evbuffer_new();
    client->kick_client = CLIENT_OK;

    strftime(buf, 248, "%Y-%m-%d %H:%M:%S", time_struct);

    // print out info about this connection
    fprintf(stdout, "%llu >> /sub connection from %s:%d %s\n", client->connection_id, req->remote_host, req->remote_port, buf);

    // Connection: Upgrade
    // Upgrade: WebSocket
    ws_upgrade = (char *) evhttp_find_header(req->input_headers, "Upgrade");
    ws_origin = (char *) evhttp_find_header(req->input_headers, "Origin");
    host = (char *) evhttp_find_header(req->input_headers, "Host");
    
    if (ps_debug && ws_upgrade) {
        fprintf(stderr, "%llu >> upgrade header is %s\n", client->connection_id, ws_upgrade);
        fprintf(stderr, "%llu >> multipart is %d\n", client->connection_id, client->multipart);
    }

    if (ws_upgrade && strstr(ws_upgrade, "WebSocket") != NULL) {
        if (ps_debug) {
            fprintf(stderr, "%llu >> upgrading connection to a websocket\n", client->connection_id);
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
                fprintf(stderr, "%llu >> setting WebSocket-Location to %s\n", client->connection_id, buf);
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
    simplehttp_set_cb("/clients", clients_cb, NULL);
    simplehttp_main(argc, argv);

    return 0;
}
