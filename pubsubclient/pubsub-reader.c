#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include "event.h"
#include "evhttp.h"
#include "pubsubclient.h"

static int debug = false;

// Pulled from AOLserver url.c
int parseurl(char *url, char **pprotocol, char **phost,
            char **pport, char **ppath, char **ptail)
{
    char           *end;

    *pprotocol = NULL;
    *phost = NULL;
    *pport = NULL;
    *ppath = NULL;
    *ptail = NULL;

    end = strchr(url, ':');
    if (end != NULL) {
        *end = '\0';
        *pprotocol = url;
        url = end + 1;
        if ((*url == '/') &&
            (*(url + 1) == '/')) {
            url = url + 2;
            *phost = url;

            end = strchr(url, ':');
            if (end != NULL) {
                *end = '\0';
                url = end + 1;
                *pport = url;
            }
            
            end = strchr(url, '/');
            if (end == NULL) {
                *ppath = "";
                *ptail = "";
                return true;
            }
            *end = '\0';
            url = end + 1;
        } else {
            url++;
        }
        *ppath = url;
        end = strrchr(url, '/');
        if (end == NULL) {
            *ptail = *ppath;
            *ppath = "";
        } else {
            *end = '\0';
            *ptail = end + 1;
        }
    } else {
        if (*url == '/') {
            url++;
            *ppath = url;

            end = strrchr(url, '/');
            if (end == NULL) {
                *ptail = *ppath;
                *ppath = "";
            } else {
                *end = '\0';
                *ptail = end + 1;
            }
        } else {
            *ptail = url;
        }
    }
    return true;
}

void callback(char *data, void *arg)
{
    fprintf(stdout, "%s\n", data);
}

int main(int argc, char **argv)
{
    char *url;
    char *protocol, *host, *port, *path, *tail, *baseprotocol,
         *basehost, *baseport, *basepath, *basetail;
    int ch;

    while ((ch = getopt(argc, argv, "d:u:")) != -1) {
        switch (ch) {
        case 'd':
            debug = true;
            break;
        case 'u':
            url = strdup(optarg);
            break;
        }
    }

    if (!url) {
        fprintf(stderr, "usage: %s -u 'http://pubsub.host:port'\n", argv[0]);
        exit(0);
    }
    parseurl(url, &protocol, &host, &port, &path, &tail);
    fprintf(stderr, "connecting to %s:%s/%s tail %s\n", host, port, path, tail);

    pubsub_to_pubsub_main(host, atoi(port), callback, NULL);
    return 1;
}
