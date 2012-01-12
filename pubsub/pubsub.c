#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <simplehttp/queue.h>
#include <simplehttp/simplehttp.h>
#include "http-internal.h"

#include <openssl/sha.h>
#include <openssl/evp.h>
#include <openssl/buffer.h>

#define BOUNDARY "xXPubSubXx"
#define MAX_PENDING_DATA 1024*1024*50
#define VERSION "1.1"

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

char *base64(const unsigned char *input, int length)
{
    BIO *bmem, *b64;
    BUF_MEM *bptr;

    b64 = BIO_new(BIO_f_base64());
    bmem = BIO_new(BIO_s_mem());
    b64 = BIO_push(b64, bmem);
    BIO_write(b64, input, length);
    BIO_flush(b64);
    BIO_get_mem_ptr(b64, &bptr);

    char *buff = (char *)malloc(bptr->length);
    memcpy(buff, bptr->data, bptr->length-1);
    buff[bptr->length-1] = 0;

    BIO_free_all(b64);

    return buff;
}

int is_slow(struct cli *client)
{
    if (client->kick_client == KICK_CLIENT) {
        return 1;
    }
    struct evhttp_connection *evcon;
    unsigned long output_buffer_length;

    evcon = (struct evhttp_connection *)client->req->evcon;
    output_buffer_length = evcon->output_buffer ? (unsigned long)EVBUFFER_LENGTH(evcon->output_buffer) : 0;
    if (output_buffer_length > MAX_PENDING_DATA) {
        kickedClients += 1;
        fprintf(stdout, "%llu >> kicking client with %lu pending data\n", client->connection_id, output_buffer_length);
        client->kick_client = KICK_CLIENT;
        // clear the clients output buffer
        evbuffer_drain(evcon->output_buffer, EVBUFFER_LENGTH(evcon->output_buffer));
        evbuffer_add_printf(evcon->output_buffer, "ERROR_TOO_SLOW. kicked for having %lu pending bytes\n", output_buffer_length);
        return 1;
    }
    return 0;
}

int can_kick(struct cli *client)
{
    if (client->kick_client == CLIENT_OK) {
        return 0;
    }
    // if the buffer length is back to zero, we can kick now
    // our error notice has been pushed to the client
    struct evhttp_connection *evcon;
    evcon = (struct evhttp_connection *)client->req->evcon;
    if (EVBUFFER_LENGTH(evcon->output_buffer) == 0) {
        return 1;
    }
    return 0;
}

void clients_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
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

void stats_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
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
    int i = 0, j = 0;
    struct cli *client;
    struct evkeyvalq args;
    int message_length = 0;
    int message_offset = 0;
    int num_messages = 0;
    char *current_message;
    int multipub;

    evhttp_parse_query(req->uri, &args);
    multipub = get_int_argument(&args, "multipub", 0);

    for (j=0; j<=EVBUFFER_LENGTH(req->input_buffer); j++)
    {
        if (j == EVBUFFER_LENGTH(req->input_buffer) || (multipub && *(EVBUFFER_DATA(req->input_buffer) + j) ==  (char)multipub))
        {
            message_length = j - message_offset ;
            current_message = EVBUFFER_DATA(req->input_buffer) + message_offset;

            msgRecv++;
            totalConns++;

            i = 0;
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
                    int ws_m = 0;
                    int ws_message_length = message_length;
                    int ws_frame_size = 64;  // Size for data fragmentation for websocket
                    while (ws_m < ws_message_length) {
                        int ws_cur_size = (ws_message_length - ws_m > ws_frame_size ? ws_frame_size : ws_message_length - ws_m);

                        int ws_code = 0;
                        if (ws_m == 0)
                            ws_code += 0x01;
                        if (ws_m + ws_cur_size >= ws_message_length)
                            ws_code += 0x80;
                        evbuffer_add_printf(client->buf, "%c", ws_code);

                        evbuffer_add_printf(client->buf, "%c", ws_cur_size);
                        evbuffer_add(client->buf, current_message + ws_m, ws_cur_size);
                        ws_m += ws_cur_size;
                    }

                } else if (client->multipart) {
                    /* chunked */
                    evbuffer_add_printf(client->buf,
                            "content-type: %s\r\ncontent-length: %d\r\n\r\n",
                            "*/*",
                            (int)message_length);
                    evbuffer_add(client->buf, current_message, message_length);
                    evbuffer_add_printf(client->buf, "\r\n--%s\r\n", BOUNDARY);
                } else {
                    /* new line terminated */
                    evbuffer_add(client->buf, current_message, message_length);
                    evbuffer_add_printf(client->buf, "\n");
                }
                evhttp_send_reply_chunk(client->req, client->buf);
                i++;
            }

            message_offset = j + 1;
            num_messages ++;
        }
    }

    evbuffer_add_printf(evb, "Published %d messages to %d clients.\n", num_messages, i);
    evhttp_send_reply(req, HTTP_OK, "OK", evb);
}

void sub_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct cli *client;
    struct evkeyvalq args;
    char *uri;
    char *ws_origin;
    char *ws_upgrade;
    char *ws_key;
    char *ws_response;
    char *host;
    char buf[248];
    struct tm *time_struct;

    currentConns++;
    totalConns++;
    evhttp_parse_query(req->uri, &args);
    client = calloc(1, sizeof(*client));
    client->multipart = get_int_argument(&args, "multipart", 1);
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
    ws_key = (char *) evhttp_find_header(req->input_headers, "Sec-WebSocket-Key");
    host = (char *) evhttp_find_header(req->input_headers, "Host");

    if (ps_debug && ws_upgrade) {
        fprintf(stderr, "%llu >> upgrade header is %s\n", client->connection_id, ws_upgrade);
        fprintf(stderr, "%llu >> multipart is %d\n", client->connection_id, client->multipart);
    }

    if (ws_upgrade && strcasestr(ws_upgrade, "WebSocket")) {
        if (ps_debug) {
            fprintf(stderr, "%llu >> upgrading connection to a websocket\n", client->connection_id);
        }
        client->req->chunked = 0;
        client->multipart = 0;
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
        if (ws_key != NULL) {
            char ws_response_tmp[SHA_DIGEST_LENGTH];
            char ws_buf[128];

            sprintf(ws_buf, "%s258EAFA5-E914-47DA-95CA-C5AB0DC85B11", ws_key);
            SHA1(ws_buf, strlen(ws_buf), ws_response_tmp);

            ws_response = base64(ws_response_tmp, SHA_DIGEST_LENGTH);
            evhttp_add_header(client->req->output_headers, "Sec-WebSocket-Accept", ws_response);
        }

        // evbuffer_add_printf(client->buf, "\r\n");
    } else if (client->multipart) {
        evhttp_add_header(client->req->output_headers, "content-type",
                "multipart/x-mixed-replace; boundary=" BOUNDARY);
        evbuffer_add_printf(client->buf, "--%s\r\n", BOUNDARY);
    } else {
        evhttp_add_header(client->req->output_headers, "content-type",
                "application/json");
        evbuffer_add_printf(client->buf, "\r\n");
    }
    if (client->websocket) {
        evhttp_send_reply_start(client->req, 101, "Switching Protocols");
    } else {
        evhttp_send_reply_start(client->req, HTTP_OK, "OK");
    }
    if (!client->websocket)
        evhttp_send_reply_chunk(client->req, client->buf);

    TAILQ_INSERT_TAIL(&clients, client, entries);
    evhttp_connection_set_closecb(req->evcon, on_close, (void *)client);
    evhttp_clear_headers(&args);
}

int version_cb(int value)
{
    fprintf(stdout, "Version: %s\n", VERSION);
    return 0;
}

int main(int argc, char **argv)
{

    define_simplehttp_options();
    option_define_bool("version", OPT_OPTIONAL, 0, NULL, version_cb, VERSION);

    if (!option_parse_command_line(argc, argv)) {
        return 1;
    }

    TAILQ_INIT(&clients);
    simplehttp_init();
    simplehttp_set_cb("/pub*", pub_cb, NULL);
    simplehttp_set_cb("/sub*", sub_cb, NULL);
    simplehttp_set_cb("/stats*", stats_cb, NULL);
    simplehttp_set_cb("/clients", clients_cb, NULL);
    simplehttp_main();
    free_options();

    return 0;
}
