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

#define NAME        "sortdb"
#define VERSION     "1.5.1"
#define DEBUG       1

void stats_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void get_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void reload_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void exit_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
char *prev_line(char *pos);
char *map_search(char *key, size_t keylen, char *lower, char *upper, int *seeks, int allow_prefix);
void info();
int main(int argc, char **argv);
void close_dbfile();
void open_dbfile();
void hup_handler(int signum);

static void *map_base = NULL;
static char *db_filename;
static struct stat st;
static char deliminator = '\t';
static int fd = 0;

enum prefix_options { disable_prefix, enable_prefix };

static uint64_t get_hits = 0;
static uint64_t get_misses = 0;
static uint64_t fwmatch_hits = 0;
static uint64_t fwmatch_misses = 0;
static uint64_t total_seeks = 0;

char *prev_line(char *pos)
{
    if (!pos) {
        return NULL;
    }
    while (pos != map_base && *(pos - 1) != '\n') {
        pos--;
    }
    return pos;
}

char *map_search(char *key, size_t keylen, char *lower, char *upper, int *seeks, int allow_prefix)
{
    ptrdiff_t distance;
    char *current;
    char *line;
    int rc;
    
    distance = (upper - lower);
    if (distance <= 1) {
        return NULL;
    }
    
    *seeks += 1;
    total_seeks++;
    current = lower + (distance / 2);
    line = prev_line(current);
    if (!line) {
        return NULL;
    }
    
    /*
    char *tmp = malloc(keylen + 1);
    memcpy(tmp, line, keylen);
    tmp[keylen] = '\0';
    if(DEBUG) fprintf(stderr, "cmp %s to %s is %d\n", key, tmp, strncmp(key, line, keylen));
    */
    
    rc = strncmp(key, line, keylen);
    if (rc < 0) {
        return map_search(key, keylen, lower, current, seeks, allow_prefix);
    } else if (rc > 0) {
        return map_search(key, keylen, current, upper, seeks, allow_prefix);
    } else if (!allow_prefix && (line[keylen] != deliminator)) {
        return map_search(key, keylen, lower, current, seeks, allow_prefix);
    } else {
        return line;
    }
}

void fwmatch_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct evkeyvalq args;
    char *key, *line, *prev, *start, *end, *newline, buf[32];
    int keylen, seeks = 0;
    
    evhttp_parse_query(req->uri, &args);
    key = (char *)evhttp_find_header(&args, "key");
    keylen = key ? strlen(key) : 0;
    
    if (DEBUG) {
        fprintf(stderr, "/fwmatch %s\n", key);
    }
    
    if (key) {
        if ((line = map_search(key, keylen, (char *)map_base, (char *)map_base + st.st_size, &seeks, enable_prefix))) {
            /*
             * Walk backwards while key prefix matches.
             * There's probably a better way to do this, however
             * this is easy and faults page in 4k chunks anyway.
             */
            while (line != (char *)map_base && (prev = prev_line(line - 1)) != line) {
                if (strncmp(key, prev, keylen) != 0) {
                    break;
                }
                line = prev;
            }
            
            /*
             * Walk forwards while key prefix matches to find all
             * records.
             */
            start = end = line;
            while ((newline = strchr(line, '\n')) != NULL
                    && newline != (char *)map_base + st.st_size) {
                line = end = newline + 1;
                if (strncmp(key, line, keylen) != 0) {
                    break;
                }
            }
            
            if (end != start) {
                // this is only supported by libevent2+
                //evbuffer_add_reference(evb, (const void *)start, (size_t)(end - start), NULL, NULL);
                evbuffer_add(evb, start, (size_t)(end - start));
            } else {
                evbuffer_add_printf(evb, "%s\n", line);
            }
            fwmatch_hits++;
            sprintf(buf, "%d", seeks);
            evhttp_add_header(req->output_headers, "x-sortdb-seeks", buf);
        } else {
            fwmatch_misses++;
        }
        
        evhttp_send_reply(req, HTTP_OK, "OK", evb);
    } else {
        evbuffer_add_printf(evb, "missing argument: key\n");
        evhttp_send_reply(req, HTTP_BADREQUEST, "MISSING_ARG_KEY", evb);
    }
    
    evhttp_clear_headers(&args);
}

void get_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct evkeyvalq args;
    char *key, *line, *newline, *delim, buf[32];
    int seeks = 0;
    
    evhttp_parse_query(req->uri, &args);
    key = (char *)evhttp_find_header(&args, "key");
    
    if (DEBUG) {
        fprintf(stderr, "/get %s\n", key);
    }
    
    if (!key) {
        evbuffer_add_printf(evb, "missing argument: key\n");
        evhttp_send_reply(req, HTTP_BADREQUEST, "MISSING_ARG_KEY", evb);
    } else if ((line = map_search(key, strlen(key), (char *)map_base, (char *)map_base + st.st_size, &seeks, disable_prefix))) {
        sprintf(buf, "%d", seeks);
        evhttp_add_header(req->output_headers, "x-sortdb-seeks", buf);
        delim = strchr(line, deliminator);
        if (delim) {
            line = delim + 1;
        }
        newline = strchr(line, '\n');
        if (newline) {
            evbuffer_add(evb, line, (newline - line) + 1);
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
}

void mget_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct evkeyvalq args;
    struct evkeyval *pair;
    char *key, *line, *newline, buf[32];
    int seeks = 0, nkeys = 0;
    
    evhttp_parse_query(req->uri, &args);
    
    TAILQ_FOREACH(pair, &args, next) {
        if (pair->key[0] != 'k') {
            continue;
        }
        key = (char *)pair->value;
        nkeys++;
        
        if (DEBUG) {
            fprintf(stderr, "/mget %s\n", key);
        }
        
        if ((line = map_search(key, strlen(key), (char *)map_base, (char *)map_base + st.st_size, &seeks, disable_prefix))) {
            newline = strchr(line, '\n');
            if (newline) {
                // this is only supported by libevent2+
                //evbuffer_add_reference(evb, (const void *)line, (size_t)(newline - line) + 1, NULL, NULL);
                evbuffer_add(evb, line, (size_t)(newline - line) + 1);
            } else {
                evbuffer_add_printf(evb, "%s\n", line);
            }
            get_hits++;
        } else {
            get_misses++;
        }
    }
    
    if (nkeys) {
        sprintf(buf, "%d", seeks);
        evhttp_add_header(req->output_headers, "x-sortdb-seeks", buf);
        evhttp_send_reply(req, HTTP_OK, "OK", evb);
    } else {
        evbuffer_add_printf(evb, "missing argument: key\n");
        evhttp_send_reply(req, HTTP_BADREQUEST, "MISSING_ARG_KEY", evb);
    }
    
    evhttp_clear_headers(&args);
}

void stats_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    int i;
    struct evkeyvalq args;
    const char *format;
    
    struct simplehttp_stats *st;
    
    st = simplehttp_stats_new();
    simplehttp_stats_get(st);
    
    evhttp_parse_query(req->uri, &args);
    format = (char *)evhttp_find_header(&args, "format");
    
    if ((format != NULL) && (strcmp(format, "json") == 0)) {
        evbuffer_add_printf(evb, "{");
        for (i = 0; i < st->callback_count; i++) {
            evbuffer_add_printf(evb, "\"%s_95\": %"PRIu64",", st->stats_labels[i], st->ninety_five_percents[i]);
            evbuffer_add_printf(evb, "\"%s_average_request\": %"PRIu64",", st->stats_labels[i], st->average_requests[i]);
            evbuffer_add_printf(evb, "\"%s_requests\": %"PRIu64",", st->stats_labels[i], st->stats_counts[i]);
        }
        evbuffer_add_printf(evb, "\"get_hits\": %"PRIu64",", get_hits);
        evbuffer_add_printf(evb, "\"get_misses\": %"PRIu64",", get_misses);
        evbuffer_add_printf(evb, "\"fwmatch_hits\": %"PRIu64",", fwmatch_hits);
        evbuffer_add_printf(evb, "\"fwmatch_misses\": %"PRIu64",", fwmatch_misses);
        evbuffer_add_printf(evb, "\"total_seeks\": %"PRIu64",", total_seeks);
        evbuffer_add_printf(evb, "\"total_requests\": %"PRIu64, st->requests);
        evbuffer_add_printf(evb, "}\n");
    } else {
        for (i = 0; i < st->callback_count; i++) {
            evbuffer_add_printf(evb, "/%s 95%%: %"PRIu64"\n", st->stats_labels[i], st->ninety_five_percents[i]);
            evbuffer_add_printf(evb, "/%s average request (usec): %"PRIu64"\n", st->stats_labels[i], st->average_requests[i]);
            evbuffer_add_printf(evb, "/%s requests: %"PRIu64"\n", st->stats_labels[i], st->stats_counts[i]);
        }
        evbuffer_add_printf(evb, "/get hits: %"PRIu64"\n", get_hits);
        evbuffer_add_printf(evb, "/get misses: %"PRIu64"\n", get_misses);
        evbuffer_add_printf(evb, "total seeks: %"PRIu64"\n", total_seeks);
        evbuffer_add_printf(evb, "total requests: %"PRIu64"\n", st->requests);
    }
    
    simplehttp_stats_free(st);
    
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
    fprintf(stdout, "%s: sorted database server.\n", NAME);
    fprintf(stdout, "Version: %s, https://github.com/bitly/simplehttp/tree/master/sortdb\n", VERSION);
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
    if (option_get_int("memory_lock") && munlock(map_base, st.st_size)) {
        fprintf(stderr, "munlock(%s) failed: %s\n", db_filename, strerror(errno));
        exit(errno);
    }
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
    if (option_get_int("memory_lock") && mlock(map_base, st.st_size)) {
        fprintf(stderr, "mlock(%s) failed: %s\n", db_filename, strerror(errno));
        exit(errno);
    }
}

int version_cb(int value)
{
    fprintf(stdout, "Version: %s\n", VERSION);
    return 0;
}

int main(int argc, char **argv)
{
    define_simplehttp_options();
    option_define_str("db_file", OPT_REQUIRED, NULL, &db_filename, NULL, NULL);
    option_define_bool("memory_lock", OPT_OPTIONAL, 0, NULL, NULL, "lock data file pages into memory");
    option_define_char("field_separator", OPT_OPTIONAL, '\t', &deliminator, NULL, "field separator (eg: comma, tab, pipe). default: TAB");
    option_define_bool("version", OPT_OPTIONAL, 0, NULL, version_cb, VERSION);
    
    if (!option_parse_command_line(argc, argv)) {
        return 1;
    }
    
    info();
    fprintf(stdout, "--field-separator is \"%c\"\n", deliminator);
    fprintf(stdout, "--db-file is %s\n", db_filename);
    
    open_dbfile();
    if (map_base == NULL) {
        exit(1);
    }
    
    simplehttp_init();
    signal(SIGHUP, hup_handler);
    simplehttp_set_cb("/get?*", get_cb, NULL);
    simplehttp_set_cb("/mget?*", mget_cb, NULL);
    simplehttp_set_cb("/fwmatch?*", fwmatch_cb, NULL);
    simplehttp_set_cb("/stats*", stats_cb, NULL);
    simplehttp_set_cb("/reload", reload_cb, NULL);
    simplehttp_set_cb("/exit", exit_cb, NULL);
    simplehttp_main();
    free_options();
    
    return 0;
}
