#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include "event.h"
#include "evhttp.h"
#include "pubsubclient.h"
#include "../server/queue.h"
#include <pthread.h>
#include <curl/curl.h>

static int debug = false;
static int encode = false;
static int threads = true;
static int numthreads = 4;
static int exiting = 0;
static char *simplequeue = "http://localhost:8080";
static pthread_mutex_t lock;

typedef struct {
    int id;
} parm;

struct data_entry {
    char *data;
    TAILQ_ENTRY(data_entry) entries;
} data_entry;
TAILQ_HEAD(data_list, data_entry) datahead;


void *thread_func(void *arg)
{
    CURL *curl;
    struct data_entry *d;
    char buf[64*1024];
    long tnum = (long)arg;

    printf("starting thread %d\n", tnum);
    curl = curl_easy_init();
    while (!exiting) {
        pthread_mutex_lock(&lock);
        d = TAILQ_LAST(&(datahead), data_list);
        if (d) TAILQ_REMOVE(&(datahead), d, entries);
        pthread_mutex_unlock(&lock);
        if (d) {
            sprintf(buf, "%s/put?data=%s", simplequeue, d->data);
            curl_easy_setopt(curl, CURLOPT_URL, buf);
            curl_easy_perform(curl); /* ignores error */
            if (d->data) free(d->data);
            free(d);
        }
        pthread_yield();
        usleep(1*1000);
    }
    printf("thread exiting %d\n", tnum);
    curl_easy_cleanup(curl);
}

void pubsub_to_pubsub_cb(char *data, void *arg)
{
    struct data_entry *d;

    if (threads) {
        d = malloc(sizeof(*d));     
        d->data = evhttp_encode_uri(data);
        pthread_mutex_lock(&lock);
        TAILQ_INSERT_TAIL(&(datahead), d, entries);
        pthread_mutex_unlock(&lock);
    } else if (encode) {
        char *s = evhttp_encode_uri(data);
        fprintf(stdout, "%s\n", s);
        free(s);
    } else {
        fprintf(stdout, "%s\n", data);
    }
}

int main(int argc, char **argv)
{
    int i, ch, port;
    char *url, host[16*1024];
    long *tidxs;
    parm *p;
    pthread_t *tids;
    pthread_attr_t pthread_custom_attr;


    while ((ch = getopt(argc, argv, "det:q:u:")) != -1) {
        switch (ch) {
        case 'd':
            debug = true;
            break;
        case 'e':
            encode = true;
            break;
        case 't':
            threads = true;
            numthreads = atoi(optarg);
            break;
        case 'q':
            threads = true;
            simplequeue = optarg;
            fprintf(stderr, "simplequeue copy enabled: %s\n", simplequeue);
        case 'u':
            url = strdup(optarg);
            break;
        }
    }

    if (!url) {
        fprintf(stderr, "usage: %s [-e|-t 4|-q 'http://localhost:8080'] -u 'http://pubsub.host:port'\n", argv[0]);
        exit(0);
    }
    if (sscanf(url, "http://%[^:]:%d", &host, &port) != 2) {
        fprintf(stderr, "pubsub must include host and port: i.e. 'http://localhost:8080'");
        exit(0);
    }

    TAILQ_INIT(&datahead);

    if (threads) {
        p=(parm *)malloc(sizeof(parm)*numthreads);

        tidxs = malloc(numthreads*sizeof(long));
	tids = (pthread_t *)malloc(numthreads*sizeof(*tids));
        pthread_attr_init(&pthread_custom_attr);
        for (i=0; i<numthreads; i++) {
            p[i].id=i;
            tidxs[i] = i;
            pthread_create(&tids[i], &pthread_custom_attr, thread_func, (void *) tidxs[i]);
        }
    }

    pubsub_to_pubsub_main(host, port, pubsub_to_pubsub_cb, NULL);

    for (i=0; i<numthreads; i++) {
        pthread_join(tids[i],NULL);
    }

    // The only reason I'm free'ing here is so I don't get hassled by the C nazis
    free(p);
    free(tidxs);
    free(tids);

    return 1;
}
