#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <simplehttp/queue.h>
#include <simplehttp/simplehttp.h>
#include <json/json.h>
#include "http-internal.h"
#include "md5.h"
#include "pcre.h"


#define DEBUG 1
#define SUCCESS 0
#define FAILURE 1
#define VERSION "1.1"
#define RECONNECT_SECS 5
#define ADDR_BUFSZ 256
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
    enum kick_client_enum kick_client;
    uint64_t connection_id;
    time_t connect_time;
    struct evbuffer *buf;
    struct evhttp_request *req;
    struct filter fltr;
    TAILQ_ENTRY(cli) entries;
} cli;

struct global_data {
    void (*cb)(char *data, void *cbarg);
    void *cbarg;
};   

char* md5_hash(const char *string);

void error_cb(int status_code, void *arg);
void source_reconnect_cb(int fd, short what, void *ctx);
void reconnect_to_source(int retryNow);
void process_message_cb(char *source, void *arg);

int parse_encrypted_fields(char *str);
int parse_blacklisted_fields(char *str);
int parse_fields(char *str, char **field_array);

int can_kick(struct cli *client);
int is_slow(struct cli *client);
void clients_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void stats_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void on_close(struct evhttp_connection *evcon, void *ctx);

TAILQ_HEAD(, cli) clients;

uint64_t totalConns = 0;
uint64_t currentConns = 0;
uint64_t kickedClients = 0;
uint64_t msgRecv = 0;
uint64_t msgSent = 0;
uint64_t number_reconnects = 0;

char *version  = "1.1";
char *g_progname = "pubsub_filtered";
struct event reconnect_ev;
struct timeval reconnect_tv = {RECONNECT_SECS,0};
struct evhttp_connection *evhttp_source_connection = NULL;
struct evhttp_request *evhttp_source_request = NULL;

char *encrypted_fields[64];
int  num_encrypted_fields = 0;
char *blacklisted_fields[64];
char *expected_key = NULL;
char *expected_value = NULL;
int expect_value=0;
int  num_blacklisted_fields = 0;


struct global_data *data = NULL;



/*
 * Parse a comma-delimited  string and populate
 * the blacklisted_fields array with the results.
 *
 * See parse_fields().
 */
int parse_blacklisted_fields(char *str)
{
    int i;

    num_blacklisted_fields = parse_fields(str, blacklisted_fields);

    for (i=0; i < num_blacklisted_fields; i++) {
        fprintf(stdout, "Blacklist field: \"%s\"\n", blacklisted_fields[i]);
    }

    return 1;
}


/*
 * Parse a comma-delimited  string and populate
 * the encrypted_fields array with the results.
 *
 * See parse_fields().
 */
int parse_encrypted_fields(char *str)
{
    int i;
    num_encrypted_fields = parse_fields(str, encrypted_fields);

    for (i=0; i < num_encrypted_fields; i++) {
        fprintf(stdout, "Encrypted field: \"%s\"\n", encrypted_fields[i]);
    }

    return 1;
}

/*
 * Parse a comma-delimited list of strings and put them
 * in an char array. Array better have enough slots
 * because I didn't have time to work out the memory allocation.
 */
int parse_fields(char *str, char **field_array)
{
    int i;
    const char delim[] = ",";
    char *tok, *str_ptr, *save_ptr;

    if (!str) return;

    str_ptr = strdup(str);

    tok = strtok_r(str_ptr, delim, &save_ptr);

    i = 0;
    while (tok != NULL) {
        field_array[i] = strdup(tok);
        tok = strtok_r(NULL, delim, &save_ptr);
        i++;
    }

    return i;
}

/* md5 encrypt a string */
char *md5_hash(const char *string)
{
    char *output = calloc(33, sizeof(char));
    struct cvs_MD5Context context;
    unsigned char checksum[16];
    int i;
    
    cvs_MD5Init (&context);
    cvs_MD5Update (&context, string, strlen(string));
    cvs_MD5Final (checksum, &context);
    for (i = 0; i < 16; i++)
    {
        sprintf(&output[i*2], "%02x", (unsigned int) checksum[i]);
    }
    output[32] = '\0';
    return output;
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
    struct cli *client;
    struct filter *fltr;
    char *subject;
    int subject_length;
    int ovector[OVECCOUNT];
    int rc, i=0;
    
    msgRecv++;
    
    json_in = json_tokener_parse(source);
    
    if (json_in == NULL) {
        fprintf(stderr, "ERR: unable to parse json %s\n", source);
        return;
    }
    
    if (expect_value) {
        element = json_object_object_get(json_in, expected_key);
        if (element == NULL) {
            json_object_put(json_in);
            return;
        }
        if (json_object_is_type(element, json_type_null)) {
            json_object_put(json_in);
            return;
        }
        raw_string = json_object_get_string(element);
        if (raw_string == NULL || !strlen(raw_string) || strcmp(raw_string, expected_value) != 0) {
            json_object_put(json_in);
            return;
        }
    }
    
    // loop through the fields we need to encrypt
    for (i=0; i < num_encrypted_fields; i++){
        field_key = encrypted_fields[i];
        element = json_object_object_get(json_in, field_key);
        if (element) {
            raw_string = json_object_get_string(element);
        } else {
            continue;
        }
        encrypted_string = md5_hash(raw_string);
        //if (DEBUG)fprintf(stdout, "encrypting %s \"%s\" => \"%s\"\n", field_key, raw_string, encrypted_string);
        json_object_object_add(json_in, field_key, json_object_new_string(encrypted_string));
        free(encrypted_string);
    }
    // loop through and remove the blacklisted fields
    for (i=0; i < num_blacklisted_fields; i++){
        field_key = blacklisted_fields[i];
        //if (DEBUG)fprintf(stdout, "removing %s\n", field_key);
        json_object_object_del(json_in, field_key);
    }
    
    json_out = json_object_to_json_string(json_in);
    //if (DEBUG)fprintf(stdout, "json_out = %d bytes\n" , strlen(json_out));
    
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
        if (client->fltr.ok) {
            fltr = &client->fltr;
            element = json_object_object_get(json_in, fltr->subject);
            if (element) {
                subject = (char *)json_object_get_string(element);
            } else {
                subject = "";
            }
            subject_length = strlen(subject);
            rc = pcre_exec(
                fltr->re,             /* the compiled pattern */
                NULL,                 /* no extra data - we didn't study the pattern */
                subject,              /* the subject string */
                subject_length,       /* the length of the subject */
                0,                    /* start at offset 0 in the subject */
                0,                    /* default options */
                ovector,              /* output vector for substring information */
                OVECCOUNT);           /* number of elements in the output vector */
            if (rc < 0) continue;
        }
        if (client->multipart) {
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
    if (client->kick_client == KICK_CLIENT) { return 1; }
    struct evhttp_connection *evcon;
    unsigned long output_buffer_length;
    
    evcon = (struct evhttp_connection *)client->req->evcon;
    output_buffer_length = (unsigned long)EVBUFFER_LENGTH(evcon->output_buffer);
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

int can_kick(struct cli *client)
{
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
        if (client->fltr.subject) free(client->fltr.subject);
        if (client->fltr.pattern) free(client->fltr.pattern);
        if (client->fltr.re) pcre_free(client->fltr.re);
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
        if (fltr->re) fltr->ok = 1;
    }

    strftime(buf, 248, "%Y-%m-%d %H:%M:%S", time_struct);

    // print out info about this connection
    fprintf(stdout, "%llu >> /sub connection from %s:%d %s\n", client->connection_id, req->remote_host, req->remote_port, buf);

    evhttp_add_header(client->req->output_headers, "content-type",
        "application/json");
    evbuffer_add_printf(client->buf, "\r\n");
    evhttp_send_reply_start(client->req, HTTP_OK, "OK");

    evhttp_send_reply_chunk(client->req, client->buf);
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
    int source_port;
    
    define_simplehttp_options();
    option_define_bool("version", OPT_OPTIONAL, 0, NULL, version_cb, VERSION);
    option_define_str("pubsub_url", OPT_REQUIRED, "http://127.0.0.1/sub?multipart=0", &pubsub_url, NULL, "url of pubsub to read from");
    option_define_str("blacklist_fields", OPT_OPTIONAL, NULL, NULL, parse_blacklisted_fields, "comma separated list of fields to remove");
    option_define_str("encrypted_fields", OPT_OPTIONAL, NULL, NULL, parse_encrypted_fields, "comma separated list of fields to encrypt");
    option_define_str("expected_key", OPT_OPTIONAL, NULL, &expected_key, NULL, "key to expect in messages before echoing to clients");
    option_define_str("expected_value", OPT_OPTIONAL, NULL, &expected_value, NULL, "value to expect in --expected-key field in messages before echoing to clients");
    
    if (!option_parse_command_line(argc, argv)){
        return 1;
    }
    
    if ((expected_value && !expected_key) || (expected_key && !expected_value)) {
        fprintf(stderr, "--expected-key and --expected-value must be used together\n");
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
    
    pubsubclient_init(source_address, source_port, source_path, process_message_cb, error_cb, data);
    simplehttp_main();
    pubsubclient_free();
    
    free_options();
    free(pubsub_url);
    free(source_address);
    free(source_path);
    
    return 0;
}
