#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "simplehttp.h"
#include "queue.h"
#include "json.h"

typedef struct cli {
    struct evbuffer *buf;
    struct evhttp_request *req;
    TAILQ_ENTRY(cli) entries;
} cli;
TAILQ_HEAD(, cli) clients;

void finalize_json(struct evhttp_request *req, struct evbuffer *evb, 
    struct evkeyvalq *args, struct json_object *jsobj);
void argtoi(struct evkeyvalq *args, char *key, int *val, int def);

uint32_t totalConns = 0;
uint32_t currentConns = 0;
uint32_t msgRecv = 0;
uint32_t msgSent = 0;

void finalize_json(struct evhttp_request *req, struct evbuffer *evb, 
    struct evkeyvalq *args, struct json_object *jsobj)
{
    char *json, *jsonp;
    
    jsonp = (char *)evhttp_find_header(args, "jsonp");
    json = json_object_to_json_string(jsobj);
    if (jsonp) {
        evbuffer_add_printf(evb, "%s(%s)\n", jsonp, json);
    } else {
        evbuffer_add_printf(evb, "%s\n", json);
    }
    json_object_put(jsobj); // Odd free function

    evhttp_send_reply(req, HTTP_OK, "OK", evb);
    evhttp_clear_headers(args);
}

void argtoi(struct evkeyvalq *args, char *key, int *val, int def)
{
    char *tmp;

    *val = def;
    tmp = (char *)evhttp_find_header(args, (const char *)key);
    if (tmp) {
        *val = atoi(tmp);
    }
}

void
stats(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct evkeyvalq args;
	struct json_object *jsobj;
	int reset;
    char *uri, *queue, *total_gets, *total_puts, *total;
    char kbuf[BUFSZ];
    
    uri = evhttp_decode_uri(req->uri);
    evhttp_parse_query(uri, &args);
    free(uri);

	argtoi(&args, "reset", &reset, 0);
	jsobj = json_object_new_object();
	json_object_object_add(jsobj, "totalConnections", json_object_new_int(totalConns));
	json_object_object_add(jsobj, "currentConnections", json_object_new_int(currentConns));
	json_object_object_add(jsobj, "messagesReceived", json_object_new_int(msgRecv));
	json_object_object_add(jsobj, "messagesSent", json_object_new_int(msgSent));
	
	if (reset) {
        totalConns = 0;
        currentConns = 0;
        msgRecv = 0;
        msgSent = 0;
    } 

	finalize_json(req, evb, &args, jsobj);
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
  
    TAILQ_FOREACH(client, &clients, entries) {
        msgSent++;
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
    currentConns++;
    totalConns++;
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
    simplehttp_set_cb("/stats*", stats, NULL);
    simplehttp_main(argc, argv);

    return 0;
}
