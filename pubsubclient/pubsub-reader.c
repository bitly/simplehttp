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


void *threadfunc(void *arg)
{
    CURL *curl;
    struct data_entry *d;
    char buf[64*1024];
    int tnum = (int *)arg;

    printf("starting thread %d\n", tnum);
    curl = curl_easy_init();
    while (!exiting) {
        pthread_mutex_lock(&lock);
        d = TAILQ_LAST(&(datahead), data_list);
        if (d) TAILQ_REMOVE(&(datahead), d, entries);
        pthread_mutex_unlock(&lock);
        if (d) {
            //printf("threadfunc: %s\n", d->data);
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


// Pulled from http://geekhideout.com/urlcode.shtml
/* Converts a hex character to its integer value */
char from_hex(char ch) {
  return isdigit(ch) ? ch - '0' : tolower(ch) - 'a' + 10;
}

/* Converts an integer value to its hex character*/
char to_hex(char code) {
  static char hex[] = "0123456789abcdef";
  return hex[code & 15];
}

/* Returns a url-encoded version of str */
/* IMPORTANT: be sure to free() the returned string after use */
char *url_encode(char *str) {
  char *pstr = str, *buf = malloc(strlen(str) * 3 + 1), *pbuf = buf;
  while (*pstr) {
    if (isalnum(*pstr) || *pstr == '-' || *pstr == '_' || *pstr == '.' || *pstr == '~') 
      *pbuf++ = *pstr;
    else if (*pstr == ' ') 
      *pbuf++ = '+';
    else 
      *pbuf++ = '%', *pbuf++ = to_hex(*pstr >> 4), *pbuf++ = to_hex(*pstr & 15);
    pstr++;
  }
  *pbuf = '\0';
  return buf;
}

/* Returns a url-decoded version of str */
/* IMPORTANT: be sure to free() the returned string after use */
char *url_decode(char *str) {
  char *pstr = str, *buf = malloc(strlen(str) + 1), *pbuf = buf;
  while (*pstr) {
    if (*pstr == '%') {
      if (pstr[1] && pstr[2]) {
        *pbuf++ = from_hex(pstr[1]) << 4 | from_hex(pstr[2]);
        pstr += 2;
      }
    } else if (*pstr == '+') { 
      *pbuf++ = ' ';
    } else {
      *pbuf++ = *pstr;
    }
    pstr++;
  }
  *pbuf = '\0';
  return buf;
}
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

void pubsub_to_pubsub_cb(char *data, void *arg)
{
    struct data_entry *d;

    if (threads) {
        d = malloc(sizeof(*d));     
        d->data = url_encode(data);
        pthread_mutex_lock(&lock);
        TAILQ_INSERT_TAIL(&(datahead), d, entries);
        pthread_mutex_unlock(&lock);
    } else if (encode) {
        char *s = url_encode(data);
        fprintf(stdout, "%s\n", s);
        free(s);
    } else {
        fprintf(stdout, "%s\n", data);
    }
}

int main(int argc, char **argv)
{
    int i, n, ch;
    char *url;
    parm *p;
    char *protocol, *host, *port, *path, *tail, *baseprotocol,
         *basehost, *baseport, *basepath, *basetail;
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
        case 'u':
            url = strdup(optarg);
            break;
        }
    }

    if (!url) {
        fprintf(stderr, "usage: %s [-e|-t 4|-q 'http://localhost:8080'] -u 'http://pubsub.host:port'\n", argv[0]);
        exit(0);
    }
    parseurl(url, &protocol, &host, &port, &path, &tail);

    TAILQ_INIT(&datahead);

    if (threads) {
        p=(parm *)malloc(sizeof(parm)*numthreads);

	tids=(pthread_t *)malloc(numthreads*sizeof(*tids));
        pthread_attr_init(&pthread_custom_attr);
        for (i=0; i<numthreads; i++) {
            p[i].id=i;
            pthread_create(&tids[i], &pthread_custom_attr, threadfunc, (void *)(i));
        }
    }

    pubsub_to_pubsub_main(host, atoi(port), pubsub_to_pubsub_cb, NULL);

    for (i=0; i<numthreads; i++) {
        pthread_join(tids[i],NULL);
    }
    free(p);

    return 1;
}
