#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
// from ../server
#include "queue.h"
#include "simplehttp.h"

struct queue_entry {
    TAILQ_ENTRY(queue_entry) entries;
    size_t bytes;
    char data[1];
};

TAILQ_HEAD(, queue_entry) queues;

char *progname = "simplequeue";
char *version = "1.1";
char *overflow_log = NULL;
FILE *overflow_log_fp = NULL;
uint64_t max_depth = 0;
size_t   max_bytes = 0;

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
    
    evhttp_parse_query(req->uri, &args);
    reset = evhttp_find_header(&args, "reset");    
    if (reset != NULL && strcmp(reset, "1") == 0) {
        depth_high_water = 0;
        n_puts = 0;
        n_gets = 0;
    } else {
        evbuffer_add_printf(evb, "puts:%lld\n", n_puts);
        evbuffer_add_printf(evb, "gets:%lld\n", n_gets);
        evbuffer_add_printf(evb, "depth:%lld\n", depth);
        evbuffer_add_printf(evb, "depth_high_water:%lld\n", depth_high_water);
        evbuffer_add_printf(evb, "bytes:%ld\n", n_bytes);
        evbuffer_add_printf(evb, "overflow:%lld\n", n_overflow);
    }
    
    evhttp_send_reply(req, HTTP_OK, "OK", evb);
    evhttp_clear_headers(&args);
}   

void
get(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct queue_entry *entry;
    
    n_gets++;
    entry = TAILQ_FIRST(&queues);
    if (entry != NULL) {
        evbuffer_add_printf(evb, "%s", entry->data);
        TAILQ_REMOVE(&queues, entry, entries);
        free(entry);
        depth--;
    }
    
    evhttp_send_reply(req, HTTP_OK, "OK", evb);
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
        evhttp_send_reply(req, HTTP_BADREQUEST, "OK", evb);
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
    fprintf(stderr, "Version %s, http://code.google.com/p/simplehttp/\n", version);
    fprintf(stderr, "\n");
    fprintf(stderr, "usage:\n");
    fprintf(stderr, "  %s -- [--overflow_log] [--max_bytes] [--max_depth]\n", progname);
    fprintf(stderr, "\n");
    exit(1);
}   

int
main(int argc, char **argv)
{
    int i;
    TAILQ_INIT(&queues);

    for (i=1; i < argc; i++) {
        if(!strcmp(argv[i], "--overflow_log")) {
            if(++i >= argc) usage();
            overflow_log = argv[i];
        } else if(!strcmp(argv[i], "--max_bytes")) {
            if(++i >= argc) usage();
            max_bytes = strtod(argv[i], (char **) NULL);
        } else if(!strcmp(argv[i], "--max_depth")) {
            if(++i >= argc) usage();
            max_depth = strtod(argv[i], (char **) NULL);
            fprintf(stdout, "max_depth set to %lld\n", max_depth);
        } else if (!strcmp(argv[i], "--help")) {
            usage();
        }
    }
    
    if (overflow_log) {
        overflow_log_fp = fopen(overflow_log, "a");
        if (!overflow_log_fp) {
            perror("fopen failed: ");
            exit(1);
        }
        fprintf(stdout, "opened overflow_log: %s\n", overflow_log);
    }

    fprintf(stderr, "Version %s, http://code.google.com/p/simplehttp/\n", version);
    fprintf(stderr, "\"%s -- --help\" for options\n", progname);
    simplehttp_init();
    signal(SIGHUP, hup_handler);
    simplehttp_set_cb("/put*", put, NULL);
    simplehttp_set_cb("/get*", get, NULL);
    simplehttp_set_cb("/dump*", dump, NULL);
    simplehttp_set_cb("/stats*", stats, NULL);
    simplehttp_main(argc, argv);
    
    if (overflow_log_fp) {
        while (depth) {
            overflow_one();
        }
        fclose(overflow_log_fp);
    }
    return 0;
}

