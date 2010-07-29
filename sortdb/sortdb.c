#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include "queue.h"
#include "simplehttp.h"


static char *progname = "sortdb";
static char *version  = "1.0";
static void *map_base = NULL;
static struct stat st;


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
    size_t distance;
    char *current;
    char *line;
    int rc;

    distance = (upper - lower);
    if (distance <= 1) return NULL;

    *seeks += 1;
    current = lower + (distance/2);
    line = prev_line(current);
    if (!line) return NULL;

    rc = strncmp(key, line, keylen);
    if (rc < 0) {
        return map_search(key, keylen, lower, current, seeks);
    } else if (rc > 0) {
        return map_search(key, keylen, current, upper, seeks);
    } else {
        return line;
    }
}

void get_cb(struct evhttp_request *req, struct evbuffer *evb,void *ctx)
{
    struct evkeyvalq args;
    char *key, *line, *newline, *tab, buf[32];
    int seeks = 0;

    evhttp_parse_query(req->uri, &args);
    key = (char *)evhttp_find_header(&args, "key");

    if (key) {
        if ((line = map_search(key, strlen(key), (char *)map_base,
             (char *)map_base+st.st_size, &seeks))) {
            sprintf(buf, "%d", seeks);
            evhttp_add_header(req->output_headers, "x-sortdb-seeks", buf);
            tab = strchr(line, '\t');
            if (tab) {
                line = tab+1;
            }
            newline = strchr(line, '\n');
            if (newline) {
                evbuffer_add(evb, line, (newline-line)+1);
            } else {
                evbuffer_add_printf(evb, "%s\n", line);
            }
            evhttp_send_reply(req, HTTP_OK, "OK", evb);
        }
        evhttp_send_reply(req, HTTP_NOTFOUND, "OK", evb);
    }
    evbuffer_add_printf(evb, "missing argument: key\n");
    evhttp_send_reply(req, HTTP_BADREQUEST, "OK", evb);
}

void usage()
{
    fprintf(stderr, "%s: Sorted database server.\n", progname);
    fprintf(stderr, "Version %s, http://code.google.com/p/simplehttp/\n", version);
    fprintf(stderr, "Provides search access to sorted tab delimited files\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "usage:\n");
    fprintf(stderr, "  %s dbfile\n", progname);
    fprintf(stderr, "\n");
    exit(1);
}

int main(int argc, char **argv)
{
    char *dbfile = argv[argc-1];
    int fd;

    if (argc < 2) {
        usage();
    }
    if ((fd = open(dbfile, O_RDONLY)) < 0) {
        fprintf(stderr, "open(%s) failed: %s\n", dbfile, strerror(errno));
        exit(errno);
    }
    if (fstat(fd, &st) < 0) {
        fprintf(stderr, "fstat(%s) failed: %s\n", dbfile, strerror(errno));
        exit(errno);
    }
    if ((map_base = mmap(0, st.st_size, PROT_READ, MAP_SHARED, fd, 0)) == MAP_FAILED) {
        fprintf(stderr, "mmap(%s) failed: %s\n", dbfile, strerror(errno));
        exit(errno);
    }

    simplehttp_init();
    simplehttp_set_cb("/get*", get_cb, NULL);
    simplehttp_main(argc, argv);
    return 0;
}
