#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <inttypes.h>
#include "simplehttp/queue.h"
#include "simplehttp/simplehttp.h"

#define VERSION "1.2"

struct queue_entry {
    TAILQ_ENTRY(queue_entry) entries;
    size_t bytes;
    char data[1];
};

TAILQ_HEAD(, queue_entry) queues;

char *progname = "simplequeue";
char *overflow_log = NULL;
FILE *overflow_log_fp = NULL;
uint64_t max_depth = 0;
size_t   max_bytes = 0;
int max_mget = 0;
char *mget_item_sep = "\n";
uint64_t depth = 0;
uint64_t depth_high_water = 0;
uint64_t n_puts = 0;
uint64_t n_gets = 0;
uint64_t n_overflow = 0;
size_t   n_bytes = 0;

void
hup_handler(int signum)
{
    signal(SIGHUP, hup_handler);
    if (overflow_log_fp) {
        fclose(overflow_log_fp);
    }
    if (overflow_log) {
        overflow_log_fp = fopen(overflow_log, "a");
        if (!overflow_log_fp) {
            perror("fopen failed: ");
            exit(1);
        }
        fprintf(stdout, "opened overflow_log: %s\n", overflow_log);
    }
}

void
overflow_one()
{
    struct queue_entry *entry;

    entry = TAILQ_FIRST(&queues);
    if (entry != NULL) {
        TAILQ_REMOVE(&queues, entry, entries);
        fwrite(entry->data, entry->bytes, 1, overflow_log_fp);
        fwrite("\n", 1, 1, overflow_log_fp);
        n_bytes -= entry->bytes;
        depth--;
        n_overflow++;
        free(entry);
    }
}

void
stats(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct evkeyvalq args;
    const char *reset;
    const char *format;
    
    evhttp_parse_query(req->uri, &args);
    reset = evhttp_find_header(&args, "reset");
    if (reset != NULL && strcmp(reset, "1") == 0) {
        depth_high_water = 0;
        n_puts = 0;
        n_gets = 0;
    } else {
        format = evhttp_find_header(&args, "format");
        
        if ((format != NULL) && (strcmp(format, "json") == 0)) {
            evbuffer_add_printf(evb, "{");
            evbuffer_add_printf(evb, "\"puts\": %"PRIu64",", n_puts);
            evbuffer_add_printf(evb, "\"gets\": %"PRIu64",", n_gets);
            evbuffer_add_printf(evb, "\"depth\": %"PRIu64",", depth);
            evbuffer_add_printf(evb, "\"depth_high_water\": %"PRIu64",", depth_high_water);
            evbuffer_add_printf(evb, "\"bytes\": %ld,", n_bytes);
            evbuffer_add_printf(evb, "\"overflow\": %"PRIu64"", n_overflow);
            evbuffer_add_printf(evb, "}\n");
        } else {
            evbuffer_add_printf(evb, "puts:%"PRIu64"\n", n_puts);
            evbuffer_add_printf(evb, "gets:%"PRIu64"\n", n_gets);
            evbuffer_add_printf(evb, "depth:%"PRIu64"\n", depth);
            evbuffer_add_printf(evb, "depth_high_water:%"PRIu64"\n", depth_high_water);
            evbuffer_add_printf(evb, "bytes:%ld\n", n_bytes);
            evbuffer_add_printf(evb, "overflow:%"PRIu64"\n", n_overflow);
        }
    }
    
    evhttp_send_reply(req, HTTP_OK, "OK", evb);
    evhttp_clear_headers(&args);
}

struct queue_entry*
get_queue_entry() 
{
    struct queue_entry *entry;
    entry = TAILQ_FIRST(&queues);
    if (entry != NULL) {
        TAILQ_REMOVE(&queues, entry, entries);
        depth--;
    }
    return entry;
}

void
get(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct queue_entry *entry;    
    n_gets++;
    
    entry = get_queue_entry();    
    if (entry != NULL) {
        evbuffer_add_printf(evb, "%s", entry->data);
        free(entry);
    }
    
    evhttp_send_reply(req, HTTP_OK, "OK", evb);
}

void
mget(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct evkeyvalq args;
    const char *items_arg;
    const char *separator;
    struct queue_entry *entry;    
    int num_items = 1;
    int i = 0;    
   
    // parse the number of items to return, defaults to 1
    evhttp_parse_query(req->uri, &args);
    items_arg = evhttp_find_header(&args, "items");
    
    // if arg, must be > 0, it is constrained to max
    if (items_arg != NULL) {
        num_items = atoi(items_arg);
        if (num_items <= 0) {
          evbuffer_add_printf(evb, "%s\n", "number of items must be > 0");
          evhttp_send_reply(req, HTTP_BADREQUEST, "ERROR", evb);
          evhttp_clear_headers(&args);   
          return;     
        } 
    }  
    if (max_mget > 0 && num_items > max_mget) {
        num_items = max_mget;
    }
    
    // allow dynamically setting separator for items, defaults to newline
    separator = evhttp_find_header(&args, "separator");
    if (separator == NULL) {
        separator = mget_item_sep;
    }
    
    // get n number of items from the queue to return
    for (i = 0; i < num_items && (entry = get_queue_entry()); n_gets++, i++) {
        evbuffer_add_printf(evb, "%s", entry->data);
        if (i < (num_items - 1)) {              
            evbuffer_add_printf(evb, "%s", separator);
        }
        free(entry);            
    }
    
    evhttp_send_reply(req, HTTP_OK, "OK", evb);
    evhttp_clear_headers(&args);
}

void
put(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct evkeyvalq args;
    struct queue_entry *entry;
    const char *data;
    size_t size;
    
    n_puts++;
    evhttp_parse_query(req->uri, &args);
    data = evhttp_find_header(&args, "data");
    if (data == NULL) {
        evbuffer_add_printf(evb, "%s\n", "missing data");
        evhttp_send_reply(req, HTTP_BADREQUEST, "ERROR", evb);
        evhttp_clear_headers(&args);
        return;
    }

    evhttp_send_reply(req, HTTP_OK, "OK", evb);

    size = strlen(data);
    entry = malloc(sizeof(*entry)+size);
    entry->bytes = size;
    strcpy(entry->data, data);
    TAILQ_INSERT_TAIL(&queues, entry, entries);
    n_bytes += size;
    depth++;
    if (depth > depth_high_water) {
        depth_high_water = depth;
    }
    while ((max_depth > 0 && depth > max_depth) 
           || (max_bytes > 0 && n_bytes > max_bytes)) {
        overflow_one();
    }
    evhttp_clear_headers(&args);
}

void
dump(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct queue_entry *entry;

    TAILQ_FOREACH(entry, &queues, entries) {
        evbuffer_add_printf(evb, "%s\n", entry->data);
    }
    
    evhttp_send_reply(req, HTTP_OK, "OK", evb);
}

void usage()
{   
    fprintf(stderr, "%s: A simple http buffer queue.\n", progname);
    fprintf(stderr, "Version %s, http://code.google.com/p/simplehttp/\n", VERSION);
    option_help();
    exit(1);
}   

int version_cb(int value) {
    fprintf(stdout, "Version: %s\n", VERSION);
    return 0;
}

int
main(int argc, char **argv)
{
    TAILQ_INIT(&queues);

    define_simplehttp_options();
    option_define_str("overflow_log", OPT_OPTIONAL, NULL, &overflow_log, NULL, "file to write data beyond --max-depth or --max-bytes");
    option_define_str("mget_item_sep", OPT_OPTIONAL, "\n", &mget_item_sep, NULL, "separator between items in mget, defaults to newline");
    // float?
    option_define_int("max_bytes", OPT_OPTIONAL, 0, NULL, NULL, "memory limit");
    option_define_int("max_depth", OPT_OPTIONAL, 0, NULL, NULL, "maximum items in queue");
    option_define_bool("version", OPT_OPTIONAL, 0, NULL, version_cb, VERSION);
    option_define_int("max_mget", OPT_OPTIONAL, 0, NULL, NULL, "maximum items to return in a single mget");
    
    if (!option_parse_command_line(argc, argv)){
        return 1;
    }
    max_bytes = (size_t)option_get_int("max_bytes");
    max_depth = (uint64_t)option_get_int("max_depth");
    max_mget = (int)option_get_int("max_mget");

    if (overflow_log) {
        overflow_log_fp = fopen(overflow_log, "a");
        if (!overflow_log_fp) {
            perror("fopen failed: ");
            exit(1);
        }
        fprintf(stdout, "opened --overflow-log: %s\n", overflow_log);
    }

    fprintf(stderr, "Version: %s, http://code.google.com/p/simplehttp/\n", VERSION);
    fprintf(stderr, "use --help for options\n");
    simplehttp_init();
    signal(SIGHUP, hup_handler);
    simplehttp_set_cb("/put*", put, NULL);
    simplehttp_set_cb("/get*", get, NULL);
    simplehttp_set_cb("/mget*", mget, NULL);
    simplehttp_set_cb("/dump*", dump, NULL);
    simplehttp_set_cb("/stats*", stats, NULL);
    simplehttp_main();
    free_options();
    
    if (overflow_log_fp) {
        while (depth) {
            overflow_one();
        }
        fclose(overflow_log_fp);
    }
    return 0;
}

