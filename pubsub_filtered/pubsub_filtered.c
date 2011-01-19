#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

#include "json/json.h"
#include "simplehttp/simplehttp.h"
#include "simplehttp/queue.h"
#include "http-internal.h"
#include "event.h"
#include "md5.h"


#define DEBUG 1
#define SUCCESS 0
#define FAILURE 1
#define LOCALHOST "127.0.0.1"
#define RECONNECT_SECS 5
#define ADDR_BUFSZ 256
#define BOUNDARY "xXPubSubXx"
#define MAX_PENDING_DATA 1024*1024*50

enum kick_client_enum {
	CLIENT_OK = 0,
	KICK_CLIENT = 1,
};

typedef struct cli {
    int multipart;
    enum kick_client_enum kick_client;
    uint64_t connection_id;
    time_t connect_time;
    struct evbuffer *buf;
    struct evhttp_request *req;
    TAILQ_ENTRY(cli) entries;
} cli;

struct global_data {
    void (*cb)(char *data, void *cbarg);
    void *cbarg;
};   


void source_callback (struct evhttp_request *req, void *arg);
void source_reconnect_cb(int fd, short what, void *ctx);
void source_req_close_cb(struct evhttp_request *req, void *arg);
void reconnect_to_source(int retryNow);
int connect_to_source();
char* md5_hash(const char *string);
void process_message_cb(struct evhttp_request *req, void *arg);

void parse_address_arg(char *optarg, char *addr, int *port);
void parse_encrypted_fields(char *str);
void parse_blacklisted_fields(char *str);
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

struct event reconnect_ev;
struct timeval reconnect_tv = {RECONNECT_SECS,0};
struct evhttp_connection *evhttp_source_connection = NULL;
struct evhttp_request *evhttp_source_request = NULL;

static char *source_path = "/sub?multipart=0";
static char source_address[ADDR_BUFSZ];
static int  source_port = 0;

static char *encrypted_fields[64];
static int  num_encrypted_fields = 0;
static char *blacklisted_fields[64];
static int  num_blacklisted_fields = 0;

struct global_data *data = NULL;

/*
 * Convenience function to parse address and port info
 * out of command line args.
 *
 */
void parse_address_arg(char *optarg, char *addr, int *port)
{
    char *ptr;
    char tmp_addr[ADDR_BUFSZ];

    ptr = strchr(optarg,':');
    if (ptr != NULL && (ptr - optarg) < strlen(optarg)) {
        sscanf(optarg, "%[^:]:%d", &tmp_addr, port);
        strcpy(addr, tmp_addr);
    } else {
        strcpy(addr, LOCALHOST);
        *port = atoi(optarg);
    }

    return;
}

/*
 * Parse a comma-delimited  string and populate
 * the blacklisted_fields array with the results.
 *
 * See parse_fields().
 */
void parse_blacklisted_fields(char *str)
{
    int i;

    num_blacklisted_fields = parse_fields(str, blacklisted_fields);

    for (i=0; i < num_blacklisted_fields; i++) {
        fprintf(stdout, "Blacklist field: \"%s\"\n", blacklisted_fields[i]);
    }

    return;
}


/*
 * Parse a comma-delimited  string and populate
 * the encrypted_fields array with the results.
 *
 * See parse_fields().
 */
void parse_encrypted_fields(char *str)
{
    int i;
    num_encrypted_fields = parse_fields(str, encrypted_fields);

    for (i=0; i < num_encrypted_fields; i++) {
        fprintf(stdout, "Encrypted field: \"%s\"\n", encrypted_fields[i]);
    }

    return;
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
char *
md5_hash(const char *string){
    char *output = calloc(32, sizeof(char));
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
    output[31] = '\0';
    return output;
}


/*
 * Callback for each fetched pubsub message.
 */
void process_message_cb(struct evhttp_request *req, void *arg) {
    if (EVBUFFER_LENGTH(req->input_buffer) < 3){
        // if (DEBUG) fprintf(stderr, "skipping\n");
        return;
    }
    if (DEBUG) fprintf(stderr, "handling %d bytes of data\n", EVBUFFER_LENGTH(req->input_buffer));
    msgRecv++;
    char *source = calloc(EVBUFFER_LENGTH(req->input_buffer), sizeof(char *));
    evbuffer_remove(req->input_buffer, source, EVBUFFER_LENGTH(req->input_buffer));
    
    struct json_object *json_in;
    char *field_key;
    const char *raw_string;
    char *encrypted_string;
    const char *json_out;
    struct cli *client;
    int i=0;
    
    if (source == NULL || strlen(source) < 3){
        free(source);
        return;
    }
    
    json_in = json_tokener_parse(source);
    
    if (json_in == NULL) {
        fprintf(stderr, "ERR: unable to parse json %s\n", source);
        free(source);
        return ;
    }
    
    // loop through the fields we need to encrypt
    for (i=0; i < num_encrypted_fields; i++){
        field_key = encrypted_fields[i];
        raw_string = json_object_get_string(json_object_object_get(json_in, field_key));
        if (!strlen(raw_string)){
            continue;
        }
        encrypted_string = md5_hash(raw_string);
        if (DEBUG)fprintf(stdout, "encrypting %s \"%s\" => \"%s\"\n", field_key, raw_string, encrypted_string);
        json_object_object_add(json_in, field_key, json_object_new_string(encrypted_string));
        free(encrypted_string);
    }
    // loop through and remove the blacklisted fields
    for (i=0; i < num_blacklisted_fields; i++){
        field_key = blacklisted_fields[i];
        if (DEBUG)fprintf(stdout, "removing %s\n", field_key);
        json_object_object_del(json_in, field_key);
    }
    
    json_out = json_object_to_json_string(json_in);
    //if (DEBUG)fprintf(stdout, "json_out = %d bytes\n" , strlen(json_out));
    
    // loop over the clients and send each this message
    TAILQ_FOREACH(client, &clients, entries) {
        msgSent++;
        // TODO: why do we need to do this?
        evbuffer_drain(client->buf, EVBUFFER_LENGTH(client->buf));
        if (is_slow(client)) {
            if (can_kick(client)) {
                evhttp_connection_free(client->req->evcon);
                continue;
            }
            continue;
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
    free(source);
    return ;
}

int
is_slow(struct cli *client) {
    if (client->kick_client == KICK_CLIENT) { return 1; }
    struct evhttp_connection *evcon;
    unsigned long output_buffer_length;
    
    evcon = (struct evhttp_connection *)client->req->evcon;
    output_buffer_length = (unsigned long)EVBUFFER_LENGTH(evcon->output_buffer);
    if (output_buffer_length > MAX_PENDING_DATA) {
        kickedClients+=1;
        fprintf(stdout, "%llu >> kicking client with %llu pending data\n", client->connection_id, output_buffer_length);
        client->kick_client = KICK_CLIENT;
        // clear the clients output buffer
        evbuffer_drain(evcon->output_buffer, EVBUFFER_LENGTH(evcon->output_buffer));
        evbuffer_add_printf(evcon->output_buffer, "ERROR_TOO_SLOW. kicked for having %llu pending bytes\n", output_buffer_length); 
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
        evbuffer_add_printf(evb, "%s:%d connected at %s. output buffer size:%llu state:%d\n", 
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
    char *reset, *uri;
    char buf[33];
    
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
    
    evbuffer_add_printf(evb, "Active connections: %llu\n", currentConns);
    evbuffer_add_printf(evb, "Total connections: %llu\n", totalConns);
    evbuffer_add_printf(evb, "Messages received: %llu\n", msgRecv);
    evbuffer_add_printf(evb, "Messages sent: %llu\n", msgSent);
    evbuffer_add_printf(evb, "Kicked clients: %llu\n", kickedClients);
    evbuffer_add_printf(evb, "Reconnects: %llu\n", number_reconnects);
    evhttp_send_reply(req, HTTP_OK, "OK", evb);
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


void sub_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct cli *client;
    char buf[248];
    struct tm *time_struct;

    currentConns++;
    totalConns++;
    client = calloc(1, sizeof(*client));
    client->multipart = 0;
    client->req = req;
    client->connection_id = totalConns;
    client->connect_time = time(NULL);
    time_struct = gmtime(&client->connect_time);
    client->buf = evbuffer_new();
    client->kick_client = CLIENT_OK;

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
}


/*
 * Callback function that gets invoked if our connection
 * to the source pubsub stream fails.
 *
 * On failure retry connection.
 *
 */
void source_req_close_cb(struct evhttp_request *req, void *arg)
{
    if (req != NULL) {
        fprintf(stderr, "HTTP STATUS: %d\n", req->response_code);
    }

    if (req == NULL || req->response_code != HTTP_OK) {
        fprintf(stderr, "Source connection failed.\n");
        reconnect_to_source(0);
    } else {
        fprintf(stderr, "Source connection closed.\n");
        reconnect_to_source(1);
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
        connect_to_source();
    } else {
        fprintf(stderr, "Reconnecting in %d secs...\n", RECONNECT_SECS);
        /* try again in RECONNECT_SECS */
        evtimer_del(&reconnect_ev);
        evtimer_set(&reconnect_ev, source_reconnect_cb, NULL);
        evtimer_add(&reconnect_ev, &reconnect_tv);
    }

    return;
}

/*
 * Callback for timer-driven reconnect.
 */
void source_reconnect_cb(int fd, short what, void *ctx)
{
    if (DEBUG) fprintf(stdout, "Timed reconnect attempt...\n");
    connect_to_source();
}

/*
 * Connect to the pubsub source stream
 *
 * This may be the initial connection or a reconnect.
 * If it's a reconnect and it fails, we'll retry every
 * RECONNECT_SECS seconds.
 *
 */
int connect_to_source()
{
    number_reconnects++;

    /*
     * clear event timer so another one doesn't fire
     * while we're connecting. A new timer will be 
     * started if we fail on this attempt.
     */
    evtimer_del(&reconnect_ev);

    // if (data == NULL) {
    //     data = calloc(1, sizeof(*data));
    //     data->cb = (void *) process_message_cb;
    //     data->cbarg = NULL;
    // }

    fprintf(stdout, "Connecting to http://%s:%d%s\n", source_address, source_port, source_path);

    /* free previously attempted connection */
    if (evhttp_source_connection) {
        evhttp_connection_free(evhttp_source_connection);
    }

    /* create new connection to source */
    evhttp_source_connection =
            evhttp_connection_new(source_address, source_port);

    if (evhttp_source_connection == NULL) {
        /*
         * This NULL check never seems to fail, even if
         * the addr:port is bogus...
         */
        fprintf(stderr, "Connection failed for source %s:%d\n", source_address, source_port);
        reconnect_to_source(0);

        return FAILURE;
    }

    evhttp_connection_set_retries(evhttp_source_connection, 1);

    evhttp_source_request = 
            evhttp_request_new(source_req_close_cb, NULL);
    evhttp_add_header(evhttp_source_request->output_headers,
            "Host", source_address);
    evhttp_request_set_chunked_cb(evhttp_source_request, process_message_cb);

    if (evhttp_make_request(evhttp_source_connection,
            evhttp_source_request, EVHTTP_REQ_GET, source_path) == -1) {
        fprintf(stdout, "REQUEST FAILED for source %s:%d%s\n", 
                source_address, source_port, source_path);
        evhttp_connection_free(evhttp_source_connection);

        reconnect_to_source(0);

        return FAILURE;
    }

    return SUCCESS;
}


void usage(){
    fprintf(stderr, "You must specify -sADDR:port -Blacklisted_fields -eEncrypt_fields [... normal pubsub options]\n");
}


int
main(int argc, char **argv)
{
    int ch;
    char *ptr;
    opterr=0;
    while ((ch = getopt(argc, argv, "s:b:e:h")) != -1) {
        if (ch == '?') {
            optind--; // re-set for next getopt() parse
            break;
        }
        switch (ch) {
        case 's':
            parse_address_arg(optarg, source_address, &source_port);
            break;
        case 'b':
            // blacklist output fields
            parse_blacklisted_fields(optarg);
            break;
        case 'e':
            // encrypt output fields
            parse_encrypted_fields(optarg);
            break;
        case 'h':
            usage();
            exit(1);
        }
    }
    if (!source_port){
        usage();
        exit(1);
    }

    TAILQ_INIT(&clients);
    simplehttp_init();
    simplehttp_set_cb("/sub", sub_cb, NULL);
    simplehttp_set_cb("/stats", stats_cb, NULL);
    simplehttp_set_cb("/clients", clients_cb, NULL);

    if (connect_to_source() == FAILURE) {
        exit(1);
    }

    simplehttp_main(argc, argv);

    return 0;
}
