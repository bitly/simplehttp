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
static int encode = false;

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

void callback(char *data, void *arg)
{
    if (encode) {
        char *s = url_encode(data);
        fprintf(stdout, "%s\n", s);
        free(s);
    } else {
        fprintf(stdout, "%s\n", data);
    }
}

int main(int argc, char **argv)
{
    char *url;
    char *protocol, *host, *port, *path, *tail, *baseprotocol,
         *basehost, *baseport, *basepath, *basetail;
    int ch;

    while ((ch = getopt(argc, argv, "deu:")) != -1) {
        switch (ch) {
        case 'd':
            debug = true;
            break;
        case 'e':
            encode = true;
            break;
        case 'u':
            url = strdup(optarg);
            break;
        }
    }

    if (!url) {
        fprintf(stderr, "usage: %s [-e] -u 'http://pubsub.host:port'\n", argv[0]);
        exit(0);
    }
    parseurl(url, &protocol, &host, &port, &path, &tail);
    fprintf(stderr, "connecting to %s:%s/%s tail %s\n", host, port, path, tail);

    pubsub_to_pubsub_main(host, atoi(port), callback, NULL);
    return 1;
}
