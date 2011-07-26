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

#define VERSION "0.2"

struct simplequeue_destination {
    char *address;
    int port;
    char *path;
    struct simplequeue_destination *next;
};

struct simplequeue_destination *sqs = NULL;
struct simplequeue_destination *cur_sq_dest = NULL;

struct simplequeue_destination *new_simplequeue_destination(char *url)
{
    struct simplequeue_destination *sq_dest;
    char *address;
    int port;
    char *path;
    
    sq_dest = malloc(sizeof(struct simplequeue_destination));
    simplehttp_parse_url(url, strlen(url), &address, &port, &path);
    _DEBUG("url: %s\n", url);
    _DEBUG("address: %s\n", address);
    _DEBUG("port: %d\n", port);
    _DEBUG("path: %s\n", path);
    free(path);
    sq_dest->address = address;
    sq_dest->port = port;
    sq_dest->path = strdup("/put?data=");
    sq_dest->next = NULL;
    
    return sq_dest;
}

void free_simplequeue_destination(struct simplequeue_destination *sq_dest)
{
    if (sq_dest) {
        free(sq_dest->address);
        free(sq_dest->path);
        free(sq_dest);
    }
}

void finish_simplequeue_put_cb(struct evhttp_request *req, void *cb_arg)
{
    _DEBUG("finish_simplequeue_put_cb()\n");
}

void process_message_cb(char *message, void *cb_arg)
{
    char *path;
    char *encoded_message;
    
    _DEBUG("process_message_cb()\n");
    
    if (message == NULL || strlen(message) < 3) {
        return;
    }
    
    if (cur_sq_dest && cur_sq_dest->next) {
        cur_sq_dest = cur_sq_dest->next;
    } else {
        cur_sq_dest = sqs;
    }
    
    encoded_message = simplehttp_encode_uri(message);
    path = malloc(10 + strlen(encoded_message) + 1); // /put?data= + encoded_message + NULL
    strcpy(path, "/put?data=");
    strcpy(path + 10, encoded_message);
    new_async_request(cur_sq_dest->address, cur_sq_dest->port, path, finish_simplequeue_put_cb, NULL);
    free(encoded_message);
    free(path);
}

int version_cb(int value)
{
    fprintf(stdout, "Version: %s\n", VERSION);
    return 0;
}

int simplequeue_url_cb(char *value)
{
    struct simplequeue_destination *sq_dest;
    
    sq_dest = new_simplequeue_destination(value);
    LL_APPEND(sqs, sq_dest);
    
    return 1;
}

void free_simplequeue_destinations()
{
    struct simplequeue_destination *sq_dest, *tmp;
    
    LL_FOREACH_SAFE(sqs, sq_dest, tmp) {
        LL_DELETE(sqs, sq_dest);
        free_simplequeue_destination(sq_dest);
    }
}

int main(int argc, char **argv)
{
    char *pubsub_url;
    char *address;
    int port;
    char *path;
    
    option_define_bool("version", OPT_OPTIONAL, 0, NULL, version_cb, VERSION);
    option_define_str("pubsub_url", OPT_REQUIRED, "http://127.0.0.1:80/sub?multipart=0", &pubsub_url, NULL, "url of pubsub to read from");
    option_define_str("simplequeue_url", OPT_REQUIRED, NULL, NULL, simplequeue_url_cb, "(multiple) url(s) of simplequeue(s) to write to");
    
    if (!option_parse_command_line(argc, argv)) {
        return 1;
    }
    
    init_async_connection_pool(1);
    
    if (simplehttp_parse_url(pubsub_url, strlen(pubsub_url), &address, &port, &path)) {
        pubsub_to_pubsub_main(address, port, path, process_message_cb, NULL);
        
        free(address);
        free(path);
    } else {
        fprintf(stderr, "ERROR: failed to parse pubsub_url\n");
    }
    
    free_simplequeue_destinations();
    free_async_connection_pool();
    free_options();
    
    return 0;
}
