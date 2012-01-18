#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <inttypes.h>
#include <leveldb/c.h>
#include <simplehttp/options.h>

#define NAME            "csv_to_leveldb"
#define VERSION         "0.1"

int db_open();
void db_close();
size_t simplehttp_get_line(FILE *fp, char **destination_buffer);
const char *simplehttp_strnchr(const char *str, size_t len, int character);

leveldb_t *ldb;
leveldb_options_t *ldb_options;
leveldb_cache_t *ldb_cache;

const char *simplehttp_strnchr(const char *str, size_t len, int character)
{
    const char *end = str + len;
    char c = (char)character;
    while (str < end) {
        if (*str == c) {
            return str;
        }
        str++;
    }
    return NULL;
}

#define SIMPLEHTTP_GET_LINE_INITIAL_INPUT_BUFFER_SZ 1024768

size_t simplehttp_get_line(FILE *fp, char **destination_buffer)
{
    static size_t cur_buffer_size = SIMPLEHTTP_GET_LINE_INITIAL_INPUT_BUFFER_SZ;
    size_t line_len;
    char *new_buffer = NULL;
    char *read_ptr;
    size_t read_size = cur_buffer_size;
    size_t new_size;
    
    if (!*destination_buffer) {
        *destination_buffer = malloc(cur_buffer_size);
        if (!*destination_buffer) {
            fprintf(stderr, "ERROR: malloc(%lu) failed\n", cur_buffer_size);
            exit(1);
        }
    }
    
    read_ptr = *destination_buffer;
    while (fgets(read_ptr, read_size, fp) != NULL) {
        line_len = strlen(*destination_buffer);
        if (((*destination_buffer)[line_len - 1] != '\n') && !feof(fp)) {
            // reallocate larger buffer
            new_size = cur_buffer_size * 2;
            new_buffer = malloc(new_size);
            if (!new_buffer) {
                fprintf(stderr, "ERROR: malloc(%lu) failed\n", new_size);
                exit(1);
            }
            memcpy(new_buffer, *destination_buffer, cur_buffer_size);
            free(*destination_buffer);
            *destination_buffer = new_buffer;
            read_size = cur_buffer_size;
            read_ptr = (*destination_buffer) + line_len;
            cur_buffer_size = new_size;
            continue;
        }
        return line_len;
    }
    
    return 0;
}
void db_close()
{
    leveldb_close(ldb);
    leveldb_options_destroy(ldb_options);
    leveldb_cache_destroy(ldb_cache);
}

int db_open()
{
    char *error = NULL;
    char *filename = option_get_str("db_file");
    
    ldb_options = leveldb_options_create();
    ldb_cache = leveldb_cache_create_lru(option_get_int("cache_size"));
    
    leveldb_options_set_create_if_missing(ldb_options, option_get_int("create_db_if_missing"));
    leveldb_options_set_error_if_exists(ldb_options, option_get_int("error_if_db_exists"));
    leveldb_options_set_paranoid_checks(ldb_options, option_get_int("paranoid_checks"));
    leveldb_options_set_write_buffer_size(ldb_options, option_get_int("write_buffer_size"));
    leveldb_options_set_block_size(ldb_options, option_get_int("block_size"));
    leveldb_options_set_cache(ldb_options, ldb_cache);
    leveldb_options_set_max_open_files(ldb_options, option_get_int("leveldb_max_open_files"));
    leveldb_options_set_block_restart_interval(ldb_options, 8);
    leveldb_options_set_compression(ldb_options, option_get_int("compression"));
    
    // leveldb_options_set_env(options, self->_env);
    leveldb_options_set_info_log(ldb_options, NULL);
    
    ldb = leveldb_open(ldb_options, filename, &error);
    if (error) {
        fprintf(stderr, "ERROR opening db:%s\n", error);
        return 0;
    }
    return 1;
}


int read_csv()
{
    leveldb_writeoptions_t *write_options;
    char *input_filename = option_get_str("input_file");
    const char *key_ptr, *val_ptr;
    size_t key_len, val_len;
    uint64_t input_row = 0;
    int stats_every = option_get_int("stats_every");
    char *err = NULL;
    char *buffer = NULL;
    char *input_deliminator = option_get_str("input_deliminator");
    int quiet = option_get_int("quiet");
    const char *comma_ptr;
    size_t buffer_len;
    FILE *input_file = stdin;
    
    if (input_filename) {
        input_file = fopen(input_filename, "r");
    }
    
    write_options = leveldb_writeoptions_create();
    
    while ((buffer_len = simplehttp_get_line(input_file, &buffer))) {
        input_row++;
        
        key_ptr = buffer;
        
        if (!quiet && (input_row > 0) && ((input_row % stats_every) == 0)) {
            fprintf(stderr, "line #%"PRIu64"\r", input_row);
        }
        
        // find the index of the first deliminator
        if ((comma_ptr = simplehttp_strnchr(buffer, buffer_len, *input_deliminator)) == NULL) {
            fprintf(stderr, "%s ERROR: LINE %"PRIu64" DELIMINATOR NOT FOUND %s\n", NAME, input_row, buffer);
            return 0;
        }
        key_len = comma_ptr - buffer;
        
        if (!key_len) {
            fprintf(stderr, "%s WARNING: SKIPPING EMPTY KEY, LINE %"PRIu64" %s\n", NAME, input_row, buffer);
            continue;
        }
        val_ptr = comma_ptr;
        val_ptr += strlen(input_deliminator);
        val_len = (buffer + buffer_len) - val_ptr;
        if (*(val_ptr + val_len - 1) == '\n') {
            val_len--;
        }
        
        leveldb_put(ldb, write_options, key_ptr, key_len, val_ptr, val_len, &err);
        if (err) {
            break;
        }
        
    }
    
    leveldb_writeoptions_destroy(write_options);
    
    if (err) {
        fprintf(stderr, "Error (LINE %"PRIu64"): %s\n", input_row, err);
        return 0;
    }
    
    return 1;
}

int version_cb(int value)
{
    fprintf(stdout, "Version: %s\n", VERSION);
    return 0;
}

int main(int argc, char **argv)
{
    option_define_bool("version", OPT_OPTIONAL, 0, NULL, version_cb, VERSION);
    option_define_str("db_file", OPT_REQUIRED, NULL, NULL, NULL, "path to leveldb file");
    option_define_bool("create_db_if_missing", OPT_OPTIONAL, 1, NULL, NULL, "Create leveldb file if missing");
    option_define_bool("error_if_db_exists", OPT_OPTIONAL, 0, NULL, NULL, "Error out if leveldb file exists");
    option_define_bool("paranoid_checks", OPT_OPTIONAL, 1, NULL, NULL, "leveldb paranoid checks");
    option_define_int("write_buffer_size", OPT_OPTIONAL, 4 << 20, NULL, NULL, "write buffer size");
    option_define_int("cache_size", OPT_OPTIONAL, 4 << 20, NULL, NULL, "cache size (frequently used blocks)");
    option_define_int("block_size", OPT_OPTIONAL, 4096, NULL, NULL, "block size");
    option_define_bool("compression", OPT_OPTIONAL, 1, NULL, NULL, "snappy compression");
    option_define_bool("verify_checksums", OPT_OPTIONAL, 1, NULL, NULL, "verify checksums at read time");
    option_define_int("leveldb_max_open_files", OPT_OPTIONAL, 4096, NULL, NULL, "leveldb max open files");
    
    option_define_str("input_file", OPT_OPTIONAL, NULL, NULL, NULL, "path to output file (default:stdin)");
    option_define_bool("quiet", OPT_OPTIONAL, 0, NULL, NULL, "quiet mode");
    option_define_int("stats_every", OPT_OPTIONAL, 1000, NULL, NULL, "show a status after processing x records");
    option_define_str("input_deliminator", OPT_OPTIONAL, ",", NULL, NULL, "input deliminator");
    
    if (!option_parse_command_line(argc, argv)) {
        return 1;
    }
    
    if (!db_open()) {
        return 1;
    }
    
    if (!read_csv()) {
        return 1;
    }
    
    db_close();
    free_options();
    
    return 0;
}
