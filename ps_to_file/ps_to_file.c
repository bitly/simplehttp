#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

#include "simplehttp/pubsubclient.h"
#include "event.h"
#include "evhttp.h"

#define DEBUG 0

struct output_metadata {
    char *filename_format;
    char current_filename[255];
    char temp_filename[255];
    FILE *output_file;
};

void
process_message_cb(char *source, void *cbarg){
    if(DEBUG) fprintf(stdout, "processing message\n");
    if (source == NULL || strlen(source) < 3){return;}

    struct output_metadata *data = (struct output_metadata *)cbarg;
    
    time_t timer = time(NULL);
    struct tm *time_struct = gmtime(&timer);
    if (DEBUG) fprintf(stdout, "strftime format %s\n", data->filename_format);
    strftime(data->temp_filename, 255, data->filename_format, time_struct);
    if (DEBUG) fprintf(stdout, "after strftime %s\n", data->temp_filename);
    if (strcmp(data->temp_filename, data->current_filename) != 0){
        if (DEBUG) fprintf(stdout, "rolling file\n");
        // roll file or open file
        if (data->output_file){
            if(DEBUG) fprintf(stdout, "closing file %s\n", data->current_filename);
            fclose(data->output_file);
        }
        if (DEBUG) fprintf(stdout, "opening file %s\n", data->temp_filename);
        strcpy(data->current_filename, data->temp_filename);
        data->output_file = fopen(data->current_filename, "ab");
    }
    
    fprintf(data->output_file,"%s\n",source);
}

void usage(){
    fprintf(stderr, "You must specify -s SOURCE_PORT -f /path/to/output_%%Y_%%m_%%d_%%H.csv\n");
    fprintf(stderr, "or -s HOST:PORT\n");
}

int
main(int argc, char **argv)
{
    int source_port;
    source_port = 0;
    char source_address[20];
    char *filename_format = NULL;
    char *ptr;
    int ch;

    while ((ch = getopt(argc, argv, "s:f:h")) != -1) {
        switch (ch) {
        case 's':
            ptr = strchr(optarg,':');
            if (ptr != NULL && (ptr - optarg) < strlen(optarg)){
                sscanf(optarg, "%[^:]:%d",&source_address, &source_port);
            }else{
                source_port = atoi(optarg);
                strcpy(source_address,"127.0.0.1");
            }
            break;
        case 'f':
            filename_format = optarg;
            fprintf(stdout,"format is %s\n",filename_format);
            break;
        case 'h':
            usage();
            exit(1);
        }
    }
    if (!source_port || !filename_format){
        usage();
        exit(1);
    }
    
    struct output_metadata *data;
    data = calloc(1,sizeof(*data));
    data->filename_format = filename_format;
    data->current_filename[0] = '\0';
    data->temp_filename[0] = '\0';
    data->output_file = NULL;

    return pubsub_to_pubsub_main(source_address, source_port, process_message_cb, data);
    
}
