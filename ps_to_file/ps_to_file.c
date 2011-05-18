#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <simplehttp/pubsubclient.h>
#include <simplehttp/simplehttp.h>

#define DEBUG 0
#define VERSION "1.1"

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

int version_cb(int value) {
    fprintf(stdout, "Version: %s\n", VERSION);
    return 0;
}

int
main(int argc, char **argv)
{
    char *source_address = "127.0.0.1";
    int source_port = 80;
    char *filename_format = NULL;
    
    define_simplehttp_options();
    option_define_bool("version", OPT_OPTIONAL, 0, NULL, version_cb, VERSION);
    option_define_str("source_host", OPT_OPTIONAL, "127.0.0.1", &source_address, NULL, NULL);
    option_define_int("source_port", OPT_OPTIONAL, 80, &source_port, NULL, NULL);
    option_define_str("filename_format", OPT_REQUIRED, NULL, &filename_format, NULL, "/var/log/pubsub.%%Y-%%m-%%d_%%H.log");
    
    if (!option_parse_command_line(argc, argv)){
        return 1;
    }
    
    struct output_metadata *data;
    data = calloc(1,sizeof(*data));
    data->filename_format = filename_format;
    data->current_filename[0] = '\0';
    data->temp_filename[0] = '\0';
    data->output_file = NULL;
    
    return pubsub_to_pubsub_main(source_address, source_port, process_message_cb, data);
    
}
