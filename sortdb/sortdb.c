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
#include <simplehttp/queue.h>
#include <simplehttp/simplehttp.h>
#include "timer/timer.h"

#define DEBUG                   1
#define NUM_REQUEST_TYPES       1
#define NUM_REQUESTS_FOR_STATS  1000

void stats_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void get_cb(struct evhttp_request *req, struct evbuffer *evb,void *ctx);
void reload_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void exit_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
char *prev_line(char *pos);
char *map_search(char *key, size_t keylen, char *lower, char *upper, int *seeks);
void usage();
void info();
int main(int argc, char **argv);
void close_dbfile();
void open_dbfile();
void hup_handler(int signum);

static char *version  = "1.2";
static void *map_base = NULL;
static char *db_filename;
static struct stat st;
static char deliminator = '\t';
static int fd = 0;

static uint64_t get_requests = 0;
static uint64_t mget_requests = 0;
static uint64_t get_hits = 0;
static uint64_t get_misses = 0;
static uint64_t total_seeks = 0;
static int64_t stats_request[NUM_REQUESTS_FOR_STATS * NUM_REQUEST_TYPES];
static int stats_request_idx[NUM_REQUEST_TYPES];

void stats_store_request(int index, unsigned int diff)
{
    stats_request[(index * NUM_REQUESTS_FOR_STATS) + stats_request_idx[index]] = diff;
    stats_request_idx[index]++;
    
    if (stats_request_idx[index] >= NUM_REQUESTS_FOR_STATS) {
        stats_request_idx[index] = 0;
    }
}

char *prev_line(char *pos)
{
    if (!pos) return NULL; 
    while (pos != map_base && *(pos-1) != '\n') {
        pos--;
    }
    return pos;
}

char *map_search(char *key, size_t keylen, char *lower, char *upper, int *seeks)
{
    ptrdiff_t distance;
    char *current;
    char *line;
    int rc;

    distance = (upper - lower);
    if (distance <= 1) return NULL;

    *seeks += 1;
    total_seeks++;
    current = lower + (distance/2);
    line = prev_line(current);
    if (!line) return NULL;

    rc = strncmp(key, line, keylen);
    if (rc < 0) {
        return map_search(key, keylen, lower, current, seeks);
    } else if (rc > 0) {
        return map_search(key, keylen, current, upper, seeks);
    } else if (line[keylen] != deliminator) {
        return map_search(key, keylen, lower, current, seeks);
    } else {
        return line;
    }
}

void get_cb(struct evhttp_request *req, struct evbuffer *evb,void *ctx)
{
    struct evkeyvalq args;
    char *uri, *key, *line, *newline, *delim, buf[32];
    char *tmp;
    int seeks = 0;
    
    _gettime(&ts1);
    
    uri = evhttp_decode_uri(req->uri);
    evhttp_parse_query(uri, &args);
    free(uri);
    key = (char *)evhttp_find_header(&args, "key");
    
    // libevent (http.c:2149) is double decoding query string params (already done at http.c:2103)
    // we dont allow spaces in keys, so convert spaces to +
    tmp = key;
    while (*tmp++ != '\0') {
        if (*tmp == ' ') {
            *tmp = '+';
        }
    } 
    
    if(DEBUG) fprintf(stderr, "/get %s\n", key);
    get_requests++;
    
    if (!key) {
        evbuffer_add_printf(evb, "missing argument: key\n");
        evhttp_send_reply(req, HTTP_BADREQUEST, "MISSING_ARG_KEY", evb);
    } else if ((line = map_search(key, strlen(key), (char *)map_base, (char *)map_base+st.st_size, &seeks))) {
        sprintf(buf, "%d", seeks);
        evhttp_add_header(req->output_headers, "x-sortdb-seeks", buf);
        delim = strchr(line, deliminator);
        if (delim) {
            line = delim+1;
        }
        newline = strchr(line, '\n');
        if (newline) {
            evbuffer_add(evb, line, (newline-line)+1);
        } else {
            evbuffer_add_printf(evb, "%s\n", line);
        }
        get_hits++;
        evhttp_send_reply(req, HTTP_OK, "OK", evb);
    } else {
        get_misses++;
        evhttp_send_reply(req, HTTP_NOTFOUND, "OK", evb);
        
    }
    
    evhttp_clear_headers(&args);
    
    _gettime(&ts2);
    stats_store_request(0, _ts_diff(ts1, ts2));
    
    return;
}

void mget_cb(struct evhttp_request *req, struct evbuffer *evb,void *ctx)
{
    struct evkeyvalq args;
    struct evkeyval *pair;
    char *uri, *key, *line, *newline, buf[32];
    char *tmp;
    int seeks = 0, nkeys = 0, nfound = 0;
    
    _gettime(&ts1);
    
    uri = evhttp_decode_uri(req->uri);
    evhttp_parse_query(uri, &args);
    free(uri);

    mget_requests++;
    TAILQ_FOREACH(pair, &args, next) {
        if (pair->key[0] != 'k') continue;
        key = (char *)pair->value;
        nkeys++;

        //key = (char *)evhttp_find_header(&args, "key");
    
        // libevent (http.c:2149) is double decoding query string params
        // (already done at http.c:2103)
        // we dont allow spaces in keys, so convert spaces to +
        tmp = key;
        while (*tmp++ != '\0') {
            if (*tmp == ' ') {
                *tmp = '+';
            }
        } 
    
        if(DEBUG) fprintf(stderr, "/mget %s\n", key);
    
        if ((line = map_search(key, strlen(key), (char *)map_base, 
            (char *)map_base+st.st_size, &seeks))) {
            newline = strchr(line, '\n');
            if (newline) {
                evbuffer_add(evb, line, (newline-line)+1);
            } else {
                evbuffer_add_printf(evb, "%s\n", line);
            }
            get_hits++;
        } else {
            get_misses++;
        }
    }

    sprintf(buf, "%d", seeks);
    evhttp_add_header(req->output_headers, "x-sortdb-seeks", buf);
    if (!nkeys) {
        evbuffer_add_printf(evb, "missing argument: key\n");
        evhttp_send_reply(req, HTTP_BADREQUEST, "MISSING_ARG_KEY", evb);
    } else if (!nfound) {
        evhttp_send_reply(req, HTTP_NOTFOUND, "OK", evb);
    } else {
        evhttp_send_reply(req, HTTP_OK, "OK", evb);
    }

    evhttp_clear_headers(&args);
    
    _gettime(&ts2);
    stats_store_request(0, _ts_diff(ts1, ts2));
    
    return;
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
        evbuffer_add_printf(evb, "\"get_requests\": %"PRIu64",", get_requests);
        evbuffer_add_printf(evb, "\"mget_requests\": %"PRIu64",", mget_requests);
        evbuffer_add_printf(evb, "\"get_hits\": %"PRIu64",", get_hits);
        evbuffer_add_printf(evb, "\"get_misses\": %"PRIu64",", get_misses);
        evbuffer_add_printf(evb, "\"total_seeks\": %"PRIu64",", total_seeks);
        evbuffer_add_printf(evb, "\"average_request\": %"PRIu64, average_request);
        evbuffer_add_printf(evb, "}\n");
    } else {
        evbuffer_add_printf(evb, "/get requests: %"PRIu64"\n", get_requests);
        evbuffer_add_printf(evb, "/mget requests: %"PRIu64"\n", mget_requests);
        evbuffer_add_printf(evb, "/get hits: %"PRIu64"\n", get_hits);
        evbuffer_add_printf(evb, "/get misses: %"PRIu64"\n", get_misses);
        evbuffer_add_printf(evb, "Total seeks: %"PRIu64"\n", total_seeks);
        evbuffer_add_printf(evb, "Avg. request (usec): %"PRIu64"\n", average_request);
    }
    
    evhttp_send_reply(req, HTTP_OK, "OK", evb);
    evhttp_clear_headers(&args);
}

void reload_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    fprintf(stdout, "/reload request recieved\n");
    close_dbfile();
    open_dbfile();
    if (map_base == NULL) {
        fprintf(stderr, "no mmaped file; exiting\n");
        exit(1);
    }
    evbuffer_add_printf(evb, "db reloaded\n");
    evhttp_send_reply(req, HTTP_OK, "OK", evb);
}

void exit_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    fprintf(stdout, "/exit request recieved\n");
    event_loopbreak();
}

void info()
{
    fprintf(stdout, "sortdb: Sorted database server.\n");
    fprintf(stdout, "Version %s, https://github.com/bitly/simplehttp/tree/master/sortdb\n", version);
}

void usage()
{
    fprintf(stderr, "Provides search access to sorted tab delimited files\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "usage: sortdb\n");
    fprintf(stderr, "\t-f /path/to/dbfile\n");
    fprintf(stderr, "\t-F \"\\t\" (field deliminator)\n");
    fprintf(stderr, "\t-a 127.0.0.1 (address to listen on)\n");
    fprintf(stderr, "\t-p 8080 (port to listen on)\n");
    fprintf(stderr, "\t-D (daemonize)\n");
    fprintf(stderr, "\n");
    exit(1);
}

void hup_handler(int signum)
{
    signal(SIGHUP, hup_handler);
    fprintf(stdout, "HUP recieved\n");
    close_dbfile();
    open_dbfile();
    if (map_base == NULL) {
        fprintf(stderr, "no mmaped file; exiting\n");
        exit(1);
    }
}

void close_dbfile()
{
    
    fprintf(stdout, "closing %s\n", db_filename);
    if (munmap(map_base, st.st_size) != 0) {
        fprintf(stderr, "failed munmap\n");
        exit(1);
    }
    if (close(fd) != 0) {
        fprintf(stderr, "failed close() on %d\n", fd);
        exit(1);
    }
    fd = 0;
    map_base = NULL;
}

void open_dbfile()
{
    if ((fd = open(db_filename, O_RDONLY)) < 0) {
        fprintf(stderr, "open(%s) failed: %s\n", db_filename, strerror(errno));
        exit(errno);
    }
    if (fstat(fd, &st) < 0) {
        fprintf(stderr, "fstat(%s) failed: %s\n", db_filename, strerror(errno));
        exit(errno);
    }
    fprintf(stdout, "opening %s\n", db_filename);
    fprintf(stdout, "db size %ld\n", (long int)st.st_size);
    if ((map_base = mmap(0, st.st_size, PROT_READ, MAP_SHARED, fd, 0)) == MAP_FAILED) {
        fprintf(stderr, "mmap(%s) failed: %s\n", db_filename, strerror(errno));
        exit(errno);
    }
}

int main(int argc, char **argv)
{
    int ch;
    
    info();
    
    opterr=0;
    while ((ch = getopt(argc, argv, "f:F:h")) != -1) {
        if (ch == '?') {
            optind--; // re-set for next getopt() parse by simplehttp_init
            break;
        }
        switch (ch) {
        case 'f':
            db_filename = optarg;
            open_dbfile();
            break;
        case 'F':
            // field deliminator
            if (strlen(optarg) != 1) {
                fprintf(stderr, "Field (-F) deliminator must be a single character");
                usage();
                exit(1);
            }
            deliminator = optarg[0];
            break;
        case 'h':
            usage();
            exit(1);
        }
    }

    if (map_base == NULL) {
        usage();
        exit(1);
    }
    
    memset(&stats_request, -1, sizeof(stats_request));
    memset(&stats_request_idx, 0, sizeof(stats_request_idx));
    
    simplehttp_init();
    signal(SIGHUP, hup_handler);
    simplehttp_set_cb("/get?*", get_cb, NULL);
    simplehttp_set_cb("/mget?*", mget_cb, NULL);
    simplehttp_set_cb("/stats*", stats_cb, NULL);
    simplehttp_set_cb("/reload", reload_cb, NULL);
    simplehttp_set_cb("/exit", exit_cb, NULL);
    simplehttp_main(argc, argv);
    
    return 0;
}
