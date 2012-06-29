#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <time.h>
#include <simplehttp/simplehttp.h>
#include <pubsubclient/pubsubclient.h>
#include <simplehttp/utlist.h>

#ifdef DEBUG
#define _DEBUG(...) fprintf(stdout, __VA_ARGS__)
#else
#define _DEBUG(...) do {;} while (0)
#endif

#define VERSION "0.5.2"

struct destination_url {
    char *address;
    int port;
    int method;
    char *path;
    struct destination_url *next;
};

struct destination_url *destinations = NULL;
struct destination_url *current_destination = NULL;
int round_robin = 0;
time_t last_message_timestamp = 0;
struct timeval max_silence_time = {0, 0};
struct event silence_ev;

struct destination_url *new_destination_url(char *url)
{
    struct destination_url *sq_dest;
    char *address;
    int port;
    char *path;
    
    sq_dest = malloc(sizeof(struct destination_url));
    simplehttp_parse_url(url, strlen(url), &address, &port, &path);
    _DEBUG("destination_url: %s\n", url);
    _DEBUG("\taddress: %s\n", address);
    _DEBUG("\tport: %d\n", port);
    _DEBUG("\tpath: %s\n", path);
    sq_dest->address = address;
    sq_dest->port = port;
    sq_dest->path = path;
    sq_dest->next = NULL;
    sq_dest->method = EVHTTP_REQ_GET;
    
    return sq_dest;
}

void free_destination_url(struct destination_url *sq_dest)
{
    if (sq_dest) {
        free(sq_dest->address);
        free(sq_dest->path);
        free(sq_dest);
    }
}

void finish_destination_cb(struct evhttp_request *req, void *cb_arg)
{
    //_DEBUG("finish_destination_cb()\n");
}

void error_cb(int status_code, void *cb_arg)
{
    event_loopbreak();
}

void silence_cb(int fd, short what, void *ctx)
{
    _DEBUG("Testing for infinite silence\n");
    if ( time(NULL) - last_message_timestamp > max_silence_time.tv_sec ) {
        _DEBUG("Things are too quiet... time to quit!\n");
        fprintf(stderr, "Exiting: No messages recieved for %lu seconds (limit: %lu seconds)\n", (time(NULL) - last_message_timestamp), max_silence_time.tv_sec);
        error_cb(-127, (void *)NULL);
    } else {
        evtimer_del(&silence_ev);
        evtimer_set(&silence_ev, silence_cb, NULL);
        evtimer_add(&silence_ev, &max_silence_time);
    }
}

void process_message_cb(char *message, void *cb_arg)
{
    struct evbuffer *evb;
    char *encoded_message;
    struct destination_url *destination;
    
    _DEBUG("process_message_cb()\n");
    
    if (message == NULL || strlen(message) < 3) {
        return;
    }
    
    if (option_get_int("max_silence") > 0) {
        last_message_timestamp = time(NULL);
    }
    
    if (!current_destination) {
        // start loop over again for round-robin
        current_destination = destinations;
    }
    LL_FOREACH(current_destination, destination) {
        if (destination->method == EVHTTP_REQ_GET) {
            evb = evbuffer_new();
            encoded_message = simplehttp_encode_uri(message);
            evbuffer_add_printf(evb, destination->path, encoded_message);
            //_DEBUG("process_message_cb(GET %s)\n", (char *)EVBUFFER_DATA(evb));
            new_async_request(destination->address, destination->port, (char *)EVBUFFER_DATA(evb),
                              finish_destination_cb, NULL);
            evbuffer_free(evb);
            free(encoded_message);
        } else {
            //_DEBUG("process_message_cb(POST %s:%d%s)\n", destination->address, destination->port, destination->path);
            new_async_request_with_body(EVHTTP_REQ_POST, destination->address, destination->port, destination->path,
                                        NULL, message, finish_destination_cb, NULL);
        }
        
        if (round_robin) {
            // break and set the next loop to start at the next destination
            current_destination = destination->next;
            break;
        }
    }
}

int version_cb(int value)
{
    fprintf(stdout, "Version: %s\n", VERSION);
    return 0;
}

int destination_get_url_cb(char *value)
{
    struct destination_url *sq_dest;
    
    if (strstr(value, "%s") == NULL) {
        fprintf(stderr, "ERROR: --destination-get-url=\"%s\" must contain a '%%s' for message data\n", value);
        return 0;
    }
    
    sq_dest = new_destination_url(value);
    sq_dest->method = EVHTTP_REQ_GET;
    LL_APPEND(destinations, sq_dest);
    
    return 1;
}

int destination_post_url_cb(char *value)
{
    struct destination_url *sq_dest;
    
    sq_dest = new_destination_url(value);
    sq_dest->method = EVHTTP_REQ_POST;
    LL_APPEND(destinations, sq_dest);
    
    return 1;
}

void free_destination_urls()
{
    struct destination_url *destination, *tmp;
    
    LL_FOREACH_SAFE(destinations, destination, tmp) {
        LL_DELETE(destinations, destination);
        free_destination_url(destination);
    }
}

int main(int argc, char **argv)
{
    char *pubsub_url;
    char *secondary_pubsub_url = NULL;
    char *address;
    int port;
    char *path;
    
    option_define_bool("version", OPT_OPTIONAL, 0, NULL, version_cb, VERSION);
    option_define_str("pubsub_url", OPT_REQUIRED, "http://127.0.0.1:80/sub?multipart=0", &pubsub_url, NULL, "url of pubsub to read from");
    option_define_str("secondary_pubsub_url", OPT_OPTIONAL, NULL, &secondary_pubsub_url, NULL, "url of pubsub to read from");
    option_define_bool("round_robin", OPT_OPTIONAL, 0, &round_robin, NULL, "write round-robin to destination urls");
    option_define_str("destination_get_url", OPT_OPTIONAL, NULL, NULL, destination_get_url_cb, "(multiple) url(s) to HTTP GET to\n\t\t\t This URL must contain a %s for the message data\n\t\t\t for a simplequeue use \"http://127.0.0.1:8080/put?data=%s\"");
    option_define_str("destination_post_url", OPT_OPTIONAL, NULL, NULL, destination_post_url_cb, "(multiple) url(s) to HTTP POST to\n\t\t\t For a pubsub endpoint use \"http://127.0.0.1:8080/pub\"");
    option_define_int("max_silence", OPT_OPTIONAL, -1, NULL, NULL, "Maximum time between pubsub messages before we disconnect and quit");
    
    if (!option_parse_command_line(argc, argv)) {
        return 1;
    }
    if (destinations == NULL) {
        fprintf(stderr, "ERROR: --destination-get-url or --destination-post-url required\n");
        return 1;
    }
    init_async_connection_pool(1);
    
    if (simplehttp_parse_url(pubsub_url, strlen(pubsub_url), &address, &port, &path)) {
        pubsubclient_init(address, port, path, process_message_cb, error_cb, NULL);
        
        if (option_get_int("max_silence") > 0) {
            _DEBUG("Registering timer.\n");
            max_silence_time.tv_sec = option_get_int("max_silence");
            evtimer_set(&silence_ev, silence_cb, NULL);
            evtimer_add(&silence_ev, &max_silence_time);
        }
        
        pubsubclient_run();
        
        free(address);
        free(path);
        free(pubsub_url);
    } else {
        fprintf(stderr, "ERROR: failed to parse pubsub_url\n");
    }
    if (secondary_pubsub_url && simplehttp_parse_url(secondary_pubsub_url, strlen(secondary_pubsub_url), &address, &port, &path)) {
	    pubsubclient_init(address, port, path, process_message_cb, error_cb, NULL);

        if (option_get_int("max_silence") > 0) {
            _DEBUG("Registering timer.\n");
            max_silence_time.tv_sec = option_get_int("max_silence");
            evtimer_set(&silence_ev, silence_cb, NULL);
            evtimer_add(&silence_ev, &max_silence_time);
        }

        pubsubclient_run();

        free(address);
        free(path);
        free(secondary_pubsub_url);
    } else {
        fprintf(stderr, "ERROR: failed to parse secondary_pubsub_url\n");
    }
    
    free_destination_urls();
    free_async_connection_pool();
    free_options();
    pubsubclient_free();
    
    return 0;
}
