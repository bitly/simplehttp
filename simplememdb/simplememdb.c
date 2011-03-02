#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <stddef.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <inttypes.h>
#include <tcutil.h>
#include <tcadb.h>
#include <simplehttp/queue.h>
#include <simplehttp/simplehttp.h>
#include "timer/timer.h"

#define NAME "simplememdb"
#define VERSION "1.0.0"
#define NUM_REQUEST_TYPES 1
#define NUM_REQUESTS_FOR_STATS 1000
#define BUFFER_SZ 1048576
#define SM_BUFFER_SZ 4096

void stats_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void exit_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void usage();
void info();

static int64_t stats_request[NUM_REQUESTS_FOR_STATS * NUM_REQUEST_TYPES];
static int stats_request_idx[NUM_REQUEST_TYPES];

TCADB *adb;

//tcadbaddint(adb, tmp_key, len, 1);
//tcadbput(adb, tmp_key, len, domain, dom_len);
//tcrdbget2(rdb, key)

void stats_store_request(int index, unsigned int diff)
{
    stats_request[(index * NUM_REQUESTS_FOR_STATS) + stats_request_idx[index]] = diff;
    stats_request_idx[index]++;
    
    if (stats_request_idx[index] >= NUM_REQUESTS_FOR_STATS) {
        stats_request_idx[index] = 0;
    }
}

void stats_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    uint64_t request_total;
    uint64_t average_request = 0;
    int i, j, c, request_array_end;
    struct evkeyvalq args;
    const char *format;
    
    for (i = 0; i < NUM_REQUEST_TYPES; i++) {
        request_total = 0;
        for (j = (i * NUM_REQUESTS_FOR_STATS), request_array_end = j + NUM_REQUESTS_FOR_STATS, c = 0; 
            (j < request_array_end) && (stats_request[j] != -1); j++, c++) {
            request_total += stats_request[j];
        }
        if (c) {
            average_request = request_total / c;
        }
    }
    
    evhttp_parse_query(req->uri, &args);
    format = (char *)evhttp_find_header(&args, "format");
    
    if ((format != NULL) && (strcmp(format, "json") == 0)) {
        evbuffer_add_printf(evb, "{");
        evbuffer_add_printf(evb, "\"average_request\": %"PRIu64, average_request);
        evbuffer_add_printf(evb, "}\n");
    } else {
        evbuffer_add_printf(evb, "Avg. request (usec): %"PRIu64"\n", average_request);
    }
    
    evhttp_send_reply(req, HTTP_OK, "OK", evb);
    evhttp_clear_headers(&args);
}

void exit_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    fprintf(stdout, "/exit request recieved\n");
    event_loopbreak();
}

void info()
{
    fprintf(stdout, "%s: simplehttp in-memory tokyo cabinet abstract database.\n", NAME);
    fprintf(stdout, "Version %s, https://github.com/bitly/simplehttp/tree/master/simplememdb\n", VERSION);
}

void usage()
{
    fprintf(stderr, "%s: simplehttp in-memory tokyo cabinet abstract database.n", NAME);
    fprintf(stderr, "\n");
    fprintf(stderr, "usage: %s\n", NAME);
    fprintf(stderr, "\t-a 127.0.0.1 (address to listen on)\n");
    fprintf(stderr, "\t-p 8080 (port to listen on)\n");
    fprintf(stderr, "\t-D (daemonize)\n");
    fprintf(stderr, "\n");
    exit(1);
}

void adb_flush()
{
    int n;
    char *key;
    void *value;
    
    tcadbiterinit(adb);
    while ((key = tcadbiternext2(adb)) != NULL) {
        value = tcadbget(adb, key, strlen(key), &n);
        if (value) {
            if ((strlen(key) > 2) && (key[0] == 'l') && (key[1] == '.')) {
                fprintf(stdout, "%s,%s\n", key, (char *)value);
            } else {
                fprintf(stdout, "%s,%d\n", key, *(int *)value);
            }
            free(value);
        }
        free(key);
    }
}

int main(int argc, char **argv)
{
    char buf[SM_BUFFER_SZ];
    unsigned long bnum = 1024;
    int ch;
    
    info();
    
    opterr=0;
    while ((ch = getopt(argc, argv, "b:vh")) != -1) {
        if (ch == '?') {
            optind--; // re-set for next getopt() parse by simplehttp_init
            break;
        }
        switch (ch) {
            case 'b':
                bnum = atoi(optarg);
                break;
            case 'v':
                exit(1);
                break;
            case 'h':
                usage();
                exit(1);
                break;
        }
    }
    
    memset(&stats_request, -1, sizeof(stats_request));
    memset(&stats_request_idx, 0, sizeof(stats_request_idx));
    
    sprintf(buf, "+#bnum=%lu", bnum);
    adb = tcadbnew();
    if (!tcadbopen(adb, buf)) {
        fprintf(stderr, "adb open error\n");
        exit(1);
    }
    
    simplehttp_init();simplehttp_set_cb("/get_int*", get_int_cb, NULL);
    simplehttp_set_cb("/get*", get_cb, NULL);
    simplehttp_set_cb("/put*", put_cb, NULL);
    simplehttp_set_cb("/del*", del_cb, NULL);
    simplehttp_set_cb("/vanish*", vanish_cb, NULL);
    simplehttp_set_cb("/fwmatch_int*", fwmatch_int_cb, NULL);
    simplehttp_set_cb("/fwmatch*", fwmatch_cb, NULL);
    simplehttp_set_cb("/incr*", incr_cb, NULL);
    simplehttp_set_cb("/stats", stats_cb, NULL);
    simplehttp_set_cb("/exit", exit_cb, NULL);
    simplehttp_main(argc, argv);
    
    tcadbclose(adb);
    tcadbdel(adb);
    
    return 0;
}
