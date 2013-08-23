#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <simplehttp/queue.h>
#include <simplehttp/simplehttp.h>
#include <pubsubclient/pubsubclient.h>
#include <json/json.h>

#include <openssl/sha.h>
#include <openssl/evp.h>
#include <openssl/buffer.h>

#include "shared.h"
#include "http-internal.h"
#include "pcre.h"


#define SUCCESS 0
#define FAILURE 1
#define VERSION "1.3"
#define RECONNECT_SECS 5
#define ADDR_BUFSZ 256
#define MAX_FIELDS 64
#define BOUNDARY "xXPubSubXx"
#define MAX_PENDING_DATA 1024*1024*50
#define OVECCOUNT 30    /* should be a multiple of 3 */
#define STRDUP(x) (x ? strdup(x) : NULL)

enum kick_client_enum {
    CLIENT_OK = 0,
    KICK_CLIENT = 1,
};

struct filter {
    int ok;
    char *subject;
    char *pattern;
    pcre *re;
    const char *error;
    int erroroffset;
};

typedef struct cli {
    int multipart;
    int websocket;
    enum kick_client_enum kick_client;
    uint64_t connection_id;
    time_t connect_time;
    struct evbuffer *buf;
    struct evhttp_request *req;
    struct filter fltr;
    TAILQ_ENTRY(cli) entries;
} cli;
TAILQ_HEAD(, cli) clients;

void error_cb(int status_code, void *arg);
void source_reconnect_cb(int fd, short what, void *ctx);
void reconnect_to_source(int retryNow);
void process_message_cb(char *source, void *arg);

int filter_message(char *subject, pcre *filter, struct json_object *json_in);

int parse_encrypted_fields(char *str);
int parse_blacklisted_fields(char *str);

int can_kick(struct cli *client);
int is_slow(struct cli *client);
void clients_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void stats_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void on_close(struct evhttp_connection *evcon, void *ctx);

static uint64_t totalConns = 0;
static uint64_t currentConns = 0;
static uint64_t kickedClients = 0;
static uint64_t msgRecv = 0;
static uint64_t msgSent = 0;
static uint64_t number_reconnects = 0;

static struct event reconnect_ev;
static struct timeval reconnect_tv = {RECONNECT_SECS, 0};
static struct evhttp_connection *evhttp_source_connection = NULL;
static struct evhttp_request *evhttp_source_request = NULL;

static char *encrypted_fields[MAX_FIELDS];
static int  num_encrypted_fields = 0;
static char *blacklisted_fields[MAX_FIELDS];
static int  num_blacklisted_fields = 0;
static char *expected_key = NULL;
static char *expected_value = NULL;
static pcre *expected_value_regex = NULL;


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
    memcpy(buff, bptr->data, bptr->length - 1);
    buff[bptr->length - 1] = 0;
    
    BIO_free_all(b64);
    
    return buff;
}

/*
 * populate the blacklisted_fields array
 */
int parse_blacklisted_fields(char *str)
{
    num_blacklisted_fields = parse_fields(str, blacklisted_fields, MAX_FIELDS, "Blacklist", stdout);
    return (num_blacklisted_fields <= MAX_FIELDS);
}

/*
 * populate the encrypted_fields array with the results.
 */
int parse_encrypted_fields(char *str)
{
    num_encrypted_fields = parse_fields(str, encrypted_fields, MAX_FIELDS, "Encrypted", stdout);
    return (num_encrypted_fields <= MAX_FIELDS);
}

/*
 * Filters a JSON object
 * return non-zero if message should be kept
 */
int filter_message(char *subject, pcre *filter, struct json_object *json_in)
{
    struct json_object *element;
    int subject_length;
    int rc;
    int ovector[OVECCOUNT];
    char *obj_subject;
    
    if (filter != NULL && subject != NULL) {
        element = json_object_object_get(json_in, subject);
        if (element) {
            obj_subject = (char *)json_object_get_string(element);
        } else {
            obj_subject = "";
        }
        subject_length = strlen(obj_subject);
        rc = pcre_exec(
                 filter,               /* the compiled pattern */
                 NULL,                 /* no extra data - we didn't study the pattern */
                 obj_subject,          /* the subject string */
                 subject_length,       /* the length of the subject */
                 0,                    /* start at offset 0 in the subject */
                 0,                    /* default options */
                 ovector,              /* output vector for substring information */
                 OVECCOUNT);           /* number of elements in the output vector */
        if (rc < 0) {
            return 0;
        }
    }
    return 1;
}

/*
 * Callback for each fetched pubsub message.
 */
void process_message_cb(char *source, void *arg)
{
    struct json_object *json_in;
    struct json_object *element;
    char *field_key;
    const char *raw_string;
    char *encrypted_string;
    const char *json_out;
    int is_heartbeat = 0; // FALSE
    struct cli *client;
    struct filter *fltr;
    char *subject;
    int subject_length;
    int ovector[OVECCOUNT];
    int rc, i = 0;
    
    msgRecv++;
    
    json_in = json_tokener_parse(source);
    
    if (json_in == NULL) {
        fprintf(stderr, "ERR: unable to parse json %s\n", source);
        return;
    }
    
    // some streams might have a heartbeat message, pass these through
    if (json_object_object_get(json_in, "_heartbeat_") != NULL) {
#ifdef DEBUG
        fprintf(stdout, "heartbeat received\n");
#endif
        is_heartbeat = 1;
    }
    
    // filter
    if (!is_heartbeat && expected_value && !filter_message_simple(expected_key, expected_value, json_in)) {
        json_object_put(json_in);
        return;
    }
    
    if (!is_heartbeat && expected_value_regex && !filter_message(expected_key, expected_value_regex, json_in)) {
        json_object_put(json_in);
        return;
    }
    
    // remove the blacklisted fields
    delete_fields(blacklisted_fields, num_blacklisted_fields, json_in);
    
    // fields we need to encrypt
    encrypt_fields(encrypted_fields, num_encrypted_fields, json_in);
    
    json_out = json_object_to_json_string(json_in);
#ifdef DEBUG
    fprintf(stdout, "json_out = %d bytes\n" , strlen(json_out));
#endif
    
    // loop over the clients and send each this message
    TAILQ_FOREACH(client, &clients, entries) {
        msgSent++;
        // TODO: why do we need to do this?
        // A: it just clears any old cruft from the clients buffer
        evbuffer_drain(client->buf, EVBUFFER_LENGTH(client->buf));
        if (is_slow(client)) {
            if (can_kick(client)) {
                evhttp_connection_free(client->req->evcon);
                continue;
            }
            continue;
        }
        // filter
        if (!is_heartbeat && client->fltr.ok && !filter_message(client->fltr.subject, client->fltr.re, json_in)) {
            continue;
        }
        if (client->websocket) {
            // set to non-chunked so that send_reply_chunked doesn't add \r\n before/after this block
            client->req->chunked = 0;
            int ws_m = 0;
            int ws_message_length = strlen(json_out);
            int ws_frame_size = 64;  // Size for data fragmentation for websocket
            while (ws_m < ws_message_length) {
                int ws_cur_size = (ws_message_length - ws_m > ws_frame_size ? ws_frame_size : ws_message_length - ws_m);
                
                int ws_code = 0;
                if (ws_m == 0) {
                    ws_code += 0x01;
                }
                if (ws_m + ws_cur_size >= ws_message_length) {
                    ws_code += 0x80;
                }
                evbuffer_add_printf(client->buf, "%c", ws_code);
                
                evbuffer_add_printf(client->buf, "%c", ws_cur_size);
                evbuffer_add(client->buf, json_out + ws_m, ws_cur_size);
                ws_m += ws_cur_size;
            }
        } else if (client->multipart) {
            /* chunked */
            evbuffer_add_printf(client->buf,
                                "content-type: %s\r\ncontent-length: %d\r\n\r\n",
                                "*/*",
                                (int)strlen(json_out));
                                
            evbuffer_add_printf(client->buf, "%s\r\n--%s\r\n", json_out, BOUNDARY);
        } else {
            /* new line terminated */
            evbuffer_add_printf(client->buf, "%s\n", json_out);
        }
        evhttp_send_reply_chunk(client->req, client->buf);
        i++;
    }
    json_object_put(json_in);
}

int is_slow(struct cli *client)
{
    if (client->kick_client == KICK_CLIENT) {
        return 1;
    }
    struct evhttp_connection *evcon;
    unsigned long output_buffer_length;
    
    evcon = (struct evhttp_connection *)client->req->evcon;
    output_buffer_length = (unsigned long)EVBUFFER_LENGTH(evcon->output_buffer);
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
    char buf[33];
    struct evkeyvalq args;
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
        evbuffer_add_printf(evb, "\"number_reconnects\": %llu", number_reconnects);
        evbuffer_add_printf(evb, "}\n");
    } else {
        evbuffer_add_printf(evb, "Active connections: %llu\n", currentConns);
        evbuffer_add_printf(evb, "Total connections: %llu\n", totalConns);
        evbuffer_add_printf(evb, "Messages received: %llu\n", msgRecv);
        evbuffer_add_printf(evb, "Messages sent: %llu\n", msgSent);
        evbuffer_add_printf(evb, "Kicked clients: %llu\n", kickedClients);
        evbuffer_add_printf(evb, "Reconnects: %llu\n", number_reconnects);
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
        if (client->fltr.subject) {
            free(client->fltr.subject);
        }
        if (client->fltr.pattern) {
            free(client->fltr.pattern);
        }
        if (client->fltr.re) {
            pcre_free(client->fltr.re);
        }
        free(client);
    } else {
        fprintf(stdout, "[unknown] >> close from  %s:%d\n", evcon->address, evcon->port);
    }
}

void sub_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct cli *client;
    struct evkeyvalq args;
    char *uri;
    char buf[248];
    struct filter *fltr;
    struct tm *time_struct;
    char *ws_origin;
    char *ws_upgrade;
    char *ws_key;
    char *ws_response;
    char *host;
    
    currentConns++;
    totalConns++;
    evhttp_parse_query(req->uri, &args);
    
    client = calloc(1, sizeof(*client));
    client->multipart = 0;
    client->req = req;
    client->connection_id = totalConns;
    client->connect_time = time(NULL);
    time_struct = gmtime(&client->connect_time);
    client->buf = evbuffer_new();
    client->kick_client = CLIENT_OK;
    
    fltr = &client->fltr;
    fltr->subject = STRDUP((char *)evhttp_find_header(&args, "filter_subject"));
    fltr->pattern = STRDUP((char *)evhttp_find_header(&args, "filter_pattern"));
    if (fltr->subject && fltr->pattern) {
        fltr->re = pcre_compile(
                       fltr->pattern,
                       0,
                       &fltr->error,
                       &fltr->erroroffset,
                       NULL);
        if (fltr->re) {
            fltr->ok = 1;
        }
    }
    
    strftime(buf, 248, "%Y-%m-%d %H:%M:%S", time_struct);
    
    // print out info about this connection
    fprintf(stdout, "%llu >> /sub connection from %s:%d %s\n", client->connection_id, req->remote_host, req->remote_port, buf);
    
    // Connection: Upgrade
    // Upgrade: WebSocket
    ws_upgrade = (char *) evhttp_find_header(req->input_headers, "Upgrade");
    ws_origin = (char *) evhttp_find_header(req->input_headers, "Origin");
    ws_key = (char *) evhttp_find_header(req->input_headers, "Sec-WebSocket-Key");
    host = (char *) evhttp_find_header(req->input_headers, "Host");
    
    if (ws_upgrade && strcasestr(ws_upgrade, "WebSocket")) {
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
    if (!client->websocket) {
        evhttp_send_reply_chunk(client->req, client->buf);
    }
    
    TAILQ_INSERT_TAIL(&clients, client, entries);
    evhttp_connection_set_closecb(req->evcon, on_close, (void *)client);
    evhttp_clear_headers(&args);
}

/*
 * Callback for timer-driven reconnect.
 */
void source_reconnect_cb(int fd, short what, void *ctx)
{
    pubsubclient_connect();
    number_reconnects++;
}

/*
 * Callback function that gets invoked if our connection
 * to the source pubsub stream fails.
 *
 * On failure retry connection.
 *
 */
void error_cb(int status_code, void *arg)
{
    fprintf(stderr, "HTTP STATUS: %d\n", status_code);
    
    if (status_code == HTTP_OK) {
        fprintf(stderr, "Source connection closed.\n");
        reconnect_to_source(1);
    } else {
        fprintf(stderr, "Source connection failed.\n");
        reconnect_to_source(0);
    }
    
    return;
}

/*
 * Reconnect function.
 *
 * IF our source pubsub stream drops us, we'll
 * try to reconnect either immediately if
 * retryNow = 1 or in RECONNECT_SECS seconds.
 *
 * See connect_to_source() and source_reconnect_cb().
 *
 */
void reconnect_to_source(int retryNow)
{

    if (retryNow) {
        fprintf(stderr, "Reconnecting now\n");
        pubsubclient_connect();
        number_reconnects++;
    } else {
        fprintf(stderr, "Reconnecting in %d secs...\n", RECONNECT_SECS);
        /* try again in RECONNECT_SECS */
        evtimer_del(&reconnect_ev);
        evtimer_set(&reconnect_ev, source_reconnect_cb, NULL);
        evtimer_add(&reconnect_ev, &reconnect_tv);
    }
    
    return;
}

int version_cb(int value)
{
    fprintf(stdout, "Version: %s\n", VERSION);
    return 0;
}

int main(int argc, char **argv)
{
    char *pubsub_url;
    char *source_address;
    char *source_path;
    char *expected_value_regex_raw = NULL;
    int source_port;
    
    define_simplehttp_options();
    option_define_bool("version", OPT_OPTIONAL, 0, NULL, version_cb, VERSION);
    option_define_str("pubsub_url", OPT_REQUIRED, "http://127.0.0.1/sub?multipart=0", &pubsub_url, NULL, "url of pubsub to read from");
    option_define_str("blacklist_fields", OPT_OPTIONAL, NULL, NULL, parse_blacklisted_fields, "comma separated list of fields to remove");
    option_define_str("encrypted_fields", OPT_OPTIONAL, NULL, NULL, parse_encrypted_fields, "comma separated list of fields to encrypt");
    option_define_str("expected_key", OPT_OPTIONAL, NULL, &expected_key, NULL, "key to expect in messages before echoing to clients");
    option_define_str("expected_value", OPT_OPTIONAL, NULL, &expected_value, NULL, "value to expect in --expected-key field in messages before echoing to clients");
    option_define_str("expected_value_regex", OPT_OPTIONAL, NULL, &expected_value_regex_raw, NULL, "regular expression matching expected value in --expected-key field before echoing to clients");
    
    if (!option_parse_command_line(argc, argv)) {
        return 1;
    }
    
    if (expected_value_regex_raw) {
        const char *tmp_err_s;
        int tmp_err_i;
        expected_value_regex = pcre_compile(
                                   expected_value_regex_raw,
                                   0,
                                   &tmp_err_s,
                                   &tmp_err_i,
                                   NULL);
        if (!expected_value_regex) {
            fprintf(stderr, "Invalid regular expression in --expected-value-regex");
            exit(1);
        }
    }
    
    if ( !!expected_key ^ !!( expected_value || expected_value_regex ) ) {
        fprintf(stderr, "--expected-key and --expected-value[-regex] must be used together\n");
        exit(1);
    }
    if (!simplehttp_parse_url(pubsub_url, strlen(pubsub_url), &source_address, &source_port, &source_path)) {
        fprintf(stderr, "ERROR: failed to parse pubsub-url\n");
        exit(1);
    }
    
    TAILQ_INIT(&clients);
    simplehttp_init();
    simplehttp_set_cb("/sub*", sub_cb, NULL);
    simplehttp_set_cb("/stats*", stats_cb, NULL);
    simplehttp_set_cb("/clients", clients_cb, NULL);
    
    pubsubclient_init(source_address, source_port, source_path, process_message_cb, error_cb, NULL);
    simplehttp_main();
    pubsubclient_free();
    
    free_options();
    free(pubsub_url);
    free(source_address);
    free(source_path);
    free_fields(blacklisted_fields, num_blacklisted_fields);
    free_fields(encrypted_fields, num_encrypted_fields);
    
    return 0;
}
