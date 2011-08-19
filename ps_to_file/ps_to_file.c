#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <simplehttp/simplehttp.h>
#include <pubsubclient/pubsubclient.h>

#ifdef DEBUG
#define _DEBUG(...) fprintf(stdout, __VA_ARGS__)
#else
#define _DEBUG(...) do {;} while (0)
#endif

#define VERSION "1.3"

struct output_metadata {
    char *filename_format;
    char current_filename[255];
    char temp_filename[255];
    FILE *output_file;
};

void process_message_cb(char *message, void *cbarg)
{
    struct output_metadata *data;
    time_t timer;
    struct tm *time_struct;
    
    _DEBUG("process_message_cb()\n");
    
    if (message == NULL || strlen(message) < 3) {
        return;
    }
    
    data = (struct output_metadata *)cbarg;
    
    timer = time(NULL);
    time_struct = gmtime(&timer);
    _DEBUG("strftime format %s\n", data->filename_format);
    strftime(data->temp_filename, 255, data->filename_format, time_struct);
    _DEBUG("after strftime %s\n", data->temp_filename);
    if (strcmp(data->temp_filename, data->current_filename) != 0) {
        _DEBUG("rolling file\n");
        // roll file or open file
        if (data->output_file) {
            _DEBUG("closing file %s\n", data->current_filename);
            fclose(data->output_file);
        }
        _DEBUG("opening file %s\n", data->temp_filename);
        strcpy(data->current_filename, data->temp_filename);
        data->output_file = fopen(data->current_filename, "ab");
    }
    
    fprintf(data->output_file, "%s\n", message);
}

void error_cb(int status_code, void *cb_arg)
{
    event_loopbreak();
}

int version_cb(int value)
{
    fprintf(stdout, "Version: %s\n", VERSION);
    return 0;
}

int main(int argc, char **argv)
{
    char *pubsub_url;
    char *address;
    int port;
    char *path;
    char *filename_format = NULL;
    struct output_metadata *data;
    
    define_simplehttp_options();
    option_define_bool("version", OPT_OPTIONAL, 0, NULL, version_cb, VERSION);
    option_define_str("pubsub_url", OPT_REQUIRED, "http://127.0.0.1:80/sub?multipart=0", &pubsub_url, NULL, "url of pubsub to read from");
    option_define_str("filename_format", OPT_REQUIRED, NULL, &filename_format, NULL, "/var/log/pubsub.%%Y-%%m-%%d_%%H.log");
    
    if (!option_parse_command_line(argc, argv)){
        return 1;
    }
    
    data = calloc(1, sizeof(struct output_metadata));
    data->filename_format = filename_format;
    data->current_filename[0] = '\0';
    data->temp_filename[0] = '\0';
    data->output_file = NULL;
    
    if (simplehttp_parse_url(pubsub_url, strlen(pubsub_url), &address, &port, &path)) {
        pubsubclient_main(address, port, path, process_message_cb, error_cb, data);
        
        if (data->output_file) {
            fclose(data->output_file);
        }
        
        free(address);
        free(path);
    } else {
        fprintf(stderr, "ERROR: failed to parse pubsub_url\n");
    }
    
    free(data);
    free_options();
    
    return 0;
}
