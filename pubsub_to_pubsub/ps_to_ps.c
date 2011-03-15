#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <simplehttp/pubsub_to_pubsub.h>
#include <simplehttp/simplehttp.h>

#define DEBUG 0

int
process_message_cb(char *source, struct evbuffer *evb, char **target_path, void *cbarg){
    if (source == NULL || strlen(source) < 3){return 0;}
    evbuffer_add_printf(evb, "%s", source);
    return 1;
}

void usage(){
    fprintf(stderr, "this script pipes data from one pubsub stream to another");
    fprintf(stderr, "You must specify -sSOURCE_PORT -tTARGET_PORT\n");
}

int
main(int argc, char **argv)
{
    int source_port, target_port;
    source_port = target_port = 0;
    int ch;
    char source_address[20];
    char target_address[20];
    char *ptr;
    
    while ((ch = getopt(argc, argv, "s:t:h")) != -1) {
        switch (ch) {
        case 's':
            ptr = strchr(optarg,':');
            if (ptr != NULL && (ptr - optarg) < strlen(optarg)){
                sscanf(optarg, "%[^:]:%d",&source_address, &source_port);
            }else{
                source_port = atoi(optarg);
                strcpy(source_address,"127.0.0.1");
            }
            fprintf(stdout, "source %s %d\n", source_address, source_port);
            break;
        case 't':
            ptr = strchr(optarg,':');
            if (ptr != NULL && (ptr - optarg) < strlen(optarg)){
                sscanf(optarg, "%[^:]:%d",&target_address, &target_port);
            }else{
                target_port = atoi(optarg);
                strcpy(target_address,"127.0.0.1");
            }
            fprintf(stdout, "target %s %d\n", target_address, target_port);
            break;
        case 'h':
            usage();
            exit(1);
        }
    }
    if (!source_port || !target_port){
        usage();
        exit(1);
    }

    return pubsub_to_pubsub_main(source_address, source_port, target_address, target_port, process_message_cb, NULL);
    
}
