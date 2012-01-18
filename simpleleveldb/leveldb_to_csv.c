#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <inttypes.h>
#include <leveldb/c.h>
#include <simplehttp/options.h>

#define NAME            "leveldb_to_csv"
#define VERSION         "0.1"

int db_open();
void db_close();

leveldb_t *ldb;
leveldb_options_t *ldb_options;
leveldb_cache_t *ldb_cache;

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


int dump_to_csv()
{
    leveldb_iterator_t *iter;
    leveldb_readoptions_t *read_options;
    char *output_filename = option_get_str("output_file");
    const char *key, *value;
    size_t key_len, value_len;
    char *err = NULL;
    char *output_deliminator = option_get_str("output_deliminator");
    FILE *out_file = stdout;
    
    if (output_filename) {
        out_file = fopen(output_filename, "w");
    }
    
    read_options = leveldb_readoptions_create();
    leveldb_readoptions_set_verify_checksums(read_options, option_get_int("verify_checksums"));
    iter = leveldb_create_iterator(ldb, read_options);
    leveldb_iter_seek_to_first(iter);
    while (leveldb_iter_valid(iter)) {
        key = leveldb_iter_key(iter, &key_len);
        value = leveldb_iter_value(iter, &value_len);
        fwrite(key, 1, key_len, out_file);
        fwrite(output_deliminator, 1, strlen(output_deliminator), out_file);
        fwrite(value, 1, value_len, out_file);
        fwrite("\n", 1, strlen("\n"), out_file);
        leveldb_iter_next(iter);
    }
    leveldb_iter_get_error(iter, &err);
    leveldb_readoptions_destroy(read_options);
    leveldb_iter_destroy(iter);
    if (err) {
        fprintf(stderr, "Error: %s\n", err);
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
    
    option_define_str("output_file", OPT_OPTIONAL, NULL, NULL, NULL, "path to output file (default:stdout)");
    option_define_str("output_deliminator", OPT_OPTIONAL, ",", NULL, NULL, "output deliminator");
    
    if (!option_parse_command_line(argc, argv)) {
        return 1;
    }
    
    if (!db_open()) {
        return 1;
    }
    
    if (!dump_to_csv()) {
        return 1;
    }
    
    db_close();
    free_options();
    
    return 0;
}
