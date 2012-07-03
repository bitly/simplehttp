#define _GNU_SOURCE // for strndup()
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <inttypes.h>
#include <simplehttp/queue.h>
#include <simplehttp/uthash.h>
#include <simplehttp/simplehttp.h>
#include <json/json.h>
#include <leveldb/c.h>

#include <sys/socket.h>
#include "http-internal.h"
#include "str_list_set.h"

// defined values
#define NAME            "simpleleveldb"
#define VERSION         "0.9.1"

#define DUMP_CSV_ITERS_CHECK       10
#define DUMP_CSV_MSECS_WORK        10
#define DUMP_CSV_MSECS_SLEEP      100
#define DUMP_CSV_MAX_BUFFER        (8*1024*1024)

const char default_sep = ',';

// extra types
enum LIST_ADD_TYPE {
    LIST_APPEND,
    LIST_PREPEND,
};

struct HashValue {
    const char *value;
    int count;
    UT_hash_handle hh;
};

// function prototypes
void finalize_request(int response_code, char *error, struct evhttp_request *req, struct evbuffer *evb, struct evkeyvalq *args, struct json_object *jsobj);
char get_argument_separator(struct evkeyvalq *args);
int db_open(void);
void db_close(void);
void del_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void put_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void mput_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void get_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void mget_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void fwmatch_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void range_match_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void stats_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void exit_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void list_add_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void list_remove_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void list_pop_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void set_add_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void set_remove_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void set_pop_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void dump_csv_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void do_dump_csv(int fd, short what, void *ctx);
void set_dump_csv_timer(struct evhttp_request *req);
void cleanup_dump_csv_cb(struct evhttp_connection *evcon, void *arg);

// global variables
leveldb_t *ldb;
leveldb_options_t *ldb_options;
leveldb_cache_t *ldb_cache;

/* options for dumping out */
const leveldb_snapshot_t *dump_snapshot;
leveldb_readoptions_t *dump_read_options;
leveldb_iterator_t *dump_iter;
struct event dump_ev;
int is_currently_dumping = 0;
char *dump_fwmatch_key;

void finalize_request(int response_code, char *error, struct evhttp_request *req, struct evbuffer *evb, struct evkeyvalq *args, struct json_object *jsobj)
{
    const char *json, *jsonp;
    int format = get_argument_format(args);
    if (error && response_code == HTTP_OK) {
        response_code = 500;
    }
    if (format == txt_format) {
        if (error) {
            if (EVBUFFER_LENGTH(evb)) {
                fprintf(stderr, "draining existing response\n");
                evbuffer_drain(evb, EVBUFFER_LENGTH(evb));
            }
            evbuffer_add_printf(evb, "DB_ERROR: %s", error);
        }
    } else {
        if (!jsobj) {
            jsobj = json_object_new_object();
        }
        if (error) {
            json_object_object_add(jsobj, "status_txt", json_object_new_string(error));
            json_object_object_add(jsobj, "status_code", json_object_new_int(response_code));
        } else {
            json_object_object_add(jsobj, "status_txt", json_object_new_string("OK"));
            json_object_object_add(jsobj, "status_code", json_object_new_int(response_code));
        }
        if (!json_object_object_get(jsobj, "data")) {
            json_object_object_add(jsobj, "data", json_object_new_string(""));
        }
        response_code = HTTP_OK;
    }
    
    if (jsobj && format == json_format) {
        jsonp = (char *)evhttp_find_header(args, "jsonp");
        json = (char *)json_object_to_json_string(jsobj);
        if (jsonp) {
            evbuffer_add_printf(evb, "%s(%s)\n", jsonp, json);
        } else {
            evbuffer_add_printf(evb, "%s\n", json);
        }
    }
    if (jsobj) {
        json_object_put(jsobj); // Odd free function
    }
    
    // don't send the request if it was already sent
    if (!req->response_code) {
        evhttp_send_reply(req, response_code, (response_code == HTTP_OK) ? "OK" : "ERROR", evb);
    } else {
        fprintf(stderr, "ERROR: request already sent\n");
    }
    evhttp_clear_headers(args);
}

char get_argument_separator(struct evkeyvalq *args)
{
    char *sep_str;
    
    sep_str = (char *)evhttp_find_header(args, "separator");
    if (sep_str) {
        if (strlen(sep_str) == 1) {
            return sep_str[0];
        } else {
            // invalid separator
            return 0;
        }
    }
    
    return default_sep;
}

void db_close(void)
{
    leveldb_close(ldb);
    leveldb_options_destroy(ldb_options);
    leveldb_cache_destroy(ldb_cache);
}

int db_open(void)
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
        free(error);
        return 0;
    }
    return 1;
}


void del_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key;
    struct evkeyvalq    args;
    struct json_object  *jsobj = NULL;
    int response_code = HTTP_OK;
    char *error = NULL;
    leveldb_writeoptions_t *write_options;
    
    evhttp_parse_query(req->uri, &args);
    
    key = (char *)evhttp_find_header(&args, "key");
    if (key == NULL) {
        finalize_request(400, "MISSING_ARG_KEY", req, evb, &args, jsobj);
        return;
    }
    
    write_options = leveldb_writeoptions_create();
    leveldb_delete(ldb, write_options, key, strlen(key), &error);
    leveldb_writeoptions_destroy(write_options);
    
    finalize_request(response_code, error, req, evb, &args, jsobj);
    free(error);
}

void mput_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *buffer_data, *term_key, *term_value;
    size_t              req_len, term_key_len, term_value_len;
    char                sep;
    struct evkeyvalq    args;
    struct json_object  *jsobj = NULL;
    int response_code = HTTP_OK;
    char *error = NULL;
    size_t sep_pos = 0, line_offset = 0, j;
    leveldb_writeoptions_t *write_options;
    
    evhttp_parse_query(req->uri, &args);
    
    sep = get_argument_separator(&args);
    
    req_len = EVBUFFER_LENGTH(req->input_buffer);
    if (req->type != EVHTTP_REQ_POST) {
        finalize_request(400, "MUST_POST_DATA", req, evb, &args, jsobj);
        return;
    } else if (req_len <= 2) {
        finalize_request(400, "MISSING_ARG_VALUE", req, evb, &args, jsobj);
        return;
    } else if (sep == 0) {
        finalize_request(400, "INVALID_SEPARATOR", req, evb, &args, jsobj);
        return;
    }
    
    write_options = leveldb_writeoptions_create();
    for (j = 0; j <= req_len; j++) {
        buffer_data = (char *) EVBUFFER_DATA(req->input_buffer);
        if (sep_pos == 0 && j < req_len && *(buffer_data + j) == sep) {
            sep_pos = j;
        } else if (j == req_len || *(buffer_data + j) ==  '\n') {
            if (line_offset == j) {
                // Do nothing... just skip this blank line
            } else if (sep_pos == 0) {
                response_code = 400;
                error = strdup("MALFORMED_CSV");
                break; // everything.
            } else {
                term_key = buffer_data + line_offset;
                term_key_len = sep_pos - line_offset;
                term_value = buffer_data + sep_pos + 1;
                term_value_len = j - (sep_pos + 1);
                
                leveldb_put(ldb, write_options, term_key, term_key_len, term_value, term_value_len, &error);
                if (error) {
                    break;
                }
            }
            line_offset = j + 1;
            sep_pos = 0;
        }
    }
    
    leveldb_writeoptions_destroy(write_options);
    finalize_request(response_code, error, req, evb, &args, jsobj);
    free(error);
}

void put_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key, *value;
    struct evkeyvalq    args;
    struct json_object  *jsobj = NULL;
    int response_code = HTTP_OK;
    char *error = NULL;
    size_t value_len;
    leveldb_writeoptions_t *write_options;
    
    evhttp_parse_query(req->uri, &args);
    
    key = (char *)evhttp_find_header(&args, "key");
    
    if (key == NULL) {
        finalize_request(400, "MISSING_ARG_KEY", req, evb, &args, jsobj);
        return;
    }
    
    if ((value = (char *)evhttp_find_header(&args, "value")) != NULL) {
        value_len = strlen(value);
    } else if (req->type == EVHTTP_REQ_POST && (value_len = EVBUFFER_LENGTH(req->input_buffer)) > 0) {
        value = (char *)EVBUFFER_DATA(req->input_buffer);
    } else {
        finalize_request(400, "MISSING_ARG_VALUE", req, evb, &args, jsobj);
        return;
    }
    
    write_options = leveldb_writeoptions_create();
    leveldb_put(ldb, write_options, key, strlen(key), value, value_len, &error);
    leveldb_writeoptions_destroy(write_options);
    
    finalize_request(response_code, error, req, evb, &args, jsobj);
    free(error);
}

void get_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key, *value;
    char                sep;
    struct evkeyvalq    args;
    struct json_object  *jsobj = NULL;
    int response_code = HTTP_OK;
    leveldb_readoptions_t *read_options;
    char *error = NULL;
    size_t vallen;
    int format;
    evhttp_parse_query(req->uri, &args);
    format = get_argument_format(&args);
    
    key = (char *)evhttp_find_header(&args, "key");
    sep = get_argument_separator(&args);
    
    if (key == NULL) {
        finalize_request(400, "MISSING_ARG_KEY", req, evb, &args, jsobj);
        return;
    }
    if (sep == 0) {
        finalize_request(400, "INVALID_SEPARATOR", req, evb, &args, jsobj);
        return;
    }
    
    read_options = leveldb_readoptions_create();
    leveldb_readoptions_set_verify_checksums(read_options, option_get_int("verify_checksums"));
    value = leveldb_get(ldb, read_options, key, strlen(key), &vallen, &error);
    leveldb_readoptions_destroy(read_options);
    
    if (value) {
        value = realloc(value, vallen + 1);
        value[vallen] = '\0';
        
        if (format == txt_format) {
            evbuffer_add_printf(evb, "%s%c%s\n", key, sep, value);
        } else {
            jsobj = json_object_new_object();
            json_object_object_add(jsobj, "data", json_object_new_string(value));
        }
        
        finalize_request(response_code, error, req, evb, &args, jsobj);
    } else {
        finalize_request(404, "NOT_FOUND", req, evb, &args, jsobj);
    }
    free(value);
    free(error);
}

void mget_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key, *value;
    char                sep;
    int                 format;
    struct evkeyvalq    args;
    struct evkeyval     *pair;
    struct json_object  *jsobj = NULL, *result_array = NULL, *tmp_obj;
    int nkeys = 0;
    int response_code = HTTP_OK;
    size_t vallen;
    char *error = NULL;
    leveldb_readoptions_t *read_options;
    
    evhttp_parse_query(req->uri, &args);
    format = get_argument_format(&args);
    
    sep = get_argument_separator(&args);
    
    if (sep == 0) {
        finalize_request(400, "INVALID_SEPARATOR", req, evb, &args, jsobj);
        return;
    }
    
    if (format == json_format) {
        jsobj = json_object_new_object();
        result_array = json_object_new_array();
        json_object_object_add(jsobj, "data", result_array);
    }
    
    read_options = leveldb_readoptions_create();
    leveldb_readoptions_set_verify_checksums(read_options, option_get_int("verify_checksums"));
    
    TAILQ_FOREACH(pair, &args, next) {
        if (pair->key[0] != 'k') {
            continue;
        }
        key = (char *)pair->value;
        nkeys++;
        
        value = leveldb_get(ldb, read_options, key, strlen(key), &vallen, &error);
        if (error) {
            break;
        }
        
        if (value) {
            value = realloc(value, vallen + 1);
            value[vallen] = '\0';
            if (format == json_format) {
                tmp_obj = json_object_new_object();
                json_object_object_add(tmp_obj, "key", json_object_new_string(key));
                json_object_object_add(tmp_obj, "value", json_object_new_string(value));
                json_object_array_add(result_array, tmp_obj);
            } else {
                evbuffer_add_printf(evb, "%s%c%s\n", key, sep, value);
            }
            free(value);
        }
    }
    
    leveldb_readoptions_destroy(read_options);
    
    if (!nkeys) {
        finalize_request(400, "MISSING_ARG_KEY", req, evb, &args, jsobj);
        return;
    }
    
    finalize_request(response_code, error, req, evb, &args, jsobj);
    free(error);
}

void range_match_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char *start_key, *end_key;
    const char *key, *value;
    char sep;
    size_t key_len, value_len;
    struct evkeyvalq args;
    struct json_object *jsobj = NULL, *result_array = NULL, *tmp_obj;
    const leveldb_snapshot_t *bt_snapshot;
    leveldb_readoptions_t *bt_read_options;
    leveldb_iterator_t *bt_iter;
    int result_count = 0, result_limit = 0, format;
    
    evhttp_parse_query(req->uri, &args);
    format = get_argument_format(&args);
    sep = get_argument_separator(&args);
    start_key = (char *)evhttp_find_header(&args, "start");
    end_key = (char *)evhttp_find_header(&args, "end");
    result_limit = get_int_argument(&args, "limit", 500);
    
    if (start_key == NULL || end_key == NULL) {
        finalize_request(400, "MISSING_ARG_KEY", req, evb, &args, jsobj);
        return;
    }
    if (strcmp(start_key, end_key) > 0) {
        finalize_request(400, "INVALID_START_KEY", req, evb, &args, jsobj);
        return;
    }
    if (sep == 0) {
        finalize_request(400, "INVALID_SEPARATOR", req, evb, &args, jsobj);
        return;
    }
    
    if (format == json_format) {
        jsobj = json_object_new_object();
        result_array = json_object_new_array();
        json_object_object_add(jsobj, "data", result_array);
    }
    
    bt_read_options = leveldb_readoptions_create();
    bt_snapshot = leveldb_create_snapshot(ldb);
    leveldb_readoptions_set_snapshot(bt_read_options, bt_snapshot);
    bt_iter = leveldb_create_iterator(ldb, bt_read_options);
    
    leveldb_iter_seek(bt_iter, start_key, strlen(start_key));
    
    while (leveldb_iter_valid(bt_iter) && (result_limit == 0 || result_count < result_limit)) {
        key = leveldb_iter_key(bt_iter, &key_len);
        
        if (strncmp(key, end_key, key_len) > 0) {
            break;
        }
        
        value = leveldb_iter_value(bt_iter, &value_len);
        
        if (format == json_format) {
            tmp_obj = json_object_new_object();
            json_object_object_add(tmp_obj, "key", json_object_new_string_len(key, key_len));
            json_object_object_add(tmp_obj, "value", json_object_new_string_len(value, value_len));
            json_object_array_add(result_array, tmp_obj);
        } else {
            evbuffer_add(evb, key, key_len);
            evbuffer_add_printf(evb, "%c", sep);
            evbuffer_add(evb, value, value_len);
            evbuffer_add_printf(evb, "\n");
        }
        
        leveldb_iter_next(bt_iter);
        result_count ++;
    }
    if (format == json_format) {
        json_object_object_add(jsobj, "status", json_object_new_string(result_count ? "ok" : "no results"));
    }
    
    finalize_request(200, NULL, req, evb, &args, jsobj);
    
    leveldb_iter_destroy(bt_iter);
    leveldb_readoptions_destroy(bt_read_options);
    leveldb_release_snapshot(ldb, bt_snapshot);
}

void fwmatch_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char *fw_key;
    const char *key, *value;
    char sep;
    size_t key_len, value_len;
    struct evkeyvalq args;
    struct json_object *jsobj = NULL, *result_array = NULL, *tmp_obj;
    const leveldb_snapshot_t *fw_snapshot;
    leveldb_readoptions_t *fw_read_options;
    leveldb_iterator_t *fw_iter;
    int result_count = 0, result_limit = 0, format;
    
    evhttp_parse_query(req->uri, &args);
    format = get_argument_format(&args);
    sep = get_argument_separator(&args);
    fw_key = (char *)evhttp_find_header(&args, "key");
    result_limit = get_int_argument(&args, "limit", 500);
    
    if (fw_key == NULL) {
        finalize_request(400, "MISSING_ARG_KEY", req, evb, &args, jsobj);
        return;
    }
    if (sep == 0) {
        finalize_request(400, "INVALID_SEPARATOR", req, evb, &args, jsobj);
        return;
    }
    
    if (format == json_format) {
        jsobj = json_object_new_object();
        result_array = json_object_new_array();
        json_object_object_add(jsobj, "data", result_array);
    }
    
    fw_read_options = leveldb_readoptions_create();
    fw_snapshot = leveldb_create_snapshot(ldb);
    leveldb_readoptions_set_snapshot(fw_read_options, fw_snapshot);
    fw_iter = leveldb_create_iterator(ldb, fw_read_options);
    
    leveldb_iter_seek(fw_iter, fw_key, strlen(fw_key));
    
    while (leveldb_iter_valid(fw_iter) && (result_limit == 0 || result_count < result_limit)) {
        key = leveldb_iter_key(fw_iter, &key_len);
        
        // this is the case where we are only fwing keys of this prefix
        // so we need to break out of the loop at the last key
        if (strlen(fw_key) > key_len || strncmp(key, fw_key, strlen(fw_key)) != 0 ) {
            break;
        }
        value = leveldb_iter_value(fw_iter, &value_len);
        
        if (format == json_format) {
            tmp_obj = json_object_new_object();
            json_object_object_add(tmp_obj, "key", json_object_new_string_len(key, key_len));
            json_object_object_add(tmp_obj, "value", json_object_new_string_len(value, value_len));
            json_object_array_add(result_array, tmp_obj);
        } else {
            evbuffer_add(evb, key, key_len);
            evbuffer_add_printf(evb, "%c", sep);
            evbuffer_add(evb, value, value_len);
            evbuffer_add_printf(evb, "\n");
        }
        
        leveldb_iter_next(fw_iter);
        result_count ++;
    }
    if (format == json_format) {
        json_object_object_add(jsobj, "status", json_object_new_string(result_count ? "ok" : "no results"));
    }
    
    finalize_request(200, NULL, req, evb, &args, jsobj);
    
    leveldb_iter_destroy(fw_iter);
    leveldb_readoptions_destroy(fw_read_options);
    leveldb_release_snapshot(ldb, fw_snapshot);
}

/* append or prepend multiple `value` strings to a string value */
void list_add_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key, *add_value, *orig_value;
    size_t              orig_valuelen;
    char                sep;
    int                 format, ret_data;
    struct evbuffer     *new_value;
    struct evkeyvalq    args;
    struct evkeyval     *arg_pair;
    struct json_object  *jsobj = NULL, *jsobj_data = NULL, *jsobj_value = NULL;
    int response_code = HTTP_OK;
    char *error = NULL;
    leveldb_readoptions_t *read_options;
    leveldb_writeoptions_t *write_options;
    enum LIST_ADD_TYPE type = (enum LIST_ADD_TYPE) ctx;
    
    evhttp_parse_query(req->uri, &args);
    format = get_argument_format(&args);
    ret_data = get_int_argument(&args, "return_data", 0);
    
    key = (char *)evhttp_find_header(&args, "key");
    add_value = (char *)evhttp_find_header(&args, "value");
    sep = get_argument_separator(&args);
    
    if (key == NULL) {
        finalize_request(400, "MISSING_ARG_KEY", req, evb, &args, jsobj);
        return;
    }
    if (add_value == NULL) {
        finalize_request(400, "MISSING_ARG_VALUE", req, evb, &args, jsobj);
        return;
    }
    if (sep == 0) {
        finalize_request(400, "INVALID_SEPARATOR", req, evb, &args, jsobj);
        return;
    }
    
    if (format == json_format) {
        jsobj = json_object_new_object();
        
        if (ret_data) {
            jsobj_data = json_object_new_object();
            jsobj_value = json_object_new_array();
            json_object_object_add(jsobj, "data", jsobj_data);
            json_object_object_add(jsobj_data, "key", json_object_new_string(key));
            json_object_object_add(jsobj_data, "value", jsobj_value);
        }
    }
    
    new_value = evbuffer_new();
    evbuffer_add_printf(new_value, "%s", ""); // null terminate
    
    read_options = leveldb_readoptions_create();
    leveldb_readoptions_set_verify_checksums(read_options, option_get_int("verify_checksums"));
    orig_value = leveldb_get(ldb, read_options, key, strlen(key), &orig_valuelen, &error);
    leveldb_readoptions_destroy(read_options);
    
    if (!error) {
        // append case - orig_value goes first
        if (type == LIST_APPEND && orig_value) {
            reserialize_list(new_value, jsobj_value, &orig_value, orig_valuelen, sep);
        }
        
        TAILQ_FOREACH(arg_pair, &args, next) {
            if (strcmp(arg_pair->key, "value") != 0) {
                continue;
            }
            // skip empty values
            if (strlen(arg_pair->value) == 0) {
                continue;
            }
            serialize_list_item(new_value, arg_pair->value, sep);
            
            if (jsobj_value) {
                json_object_array_add(jsobj_value, json_object_new_string(arg_pair->value));
            }
        }
        
        // prepend case - orig_value goes last
        if (type == LIST_PREPEND && orig_value) {
            reserialize_list(new_value, jsobj_value, &orig_value, orig_valuelen, sep);
        }
        
        write_options = leveldb_writeoptions_create();
        leveldb_put(ldb, write_options, key, strlen(key), (char *)EVBUFFER_DATA(new_value), EVBUFFER_LENGTH(new_value), &error);
        leveldb_writeoptions_destroy(write_options);
    }
    
    if (ret_data && format == txt_format) {
        evbuffer_add_printf(evb, "%s%c%s\n", key, sep, EVBUFFER_DATA(new_value));
    }
    
    finalize_request(response_code, error, req, evb, &args, jsobj);
    evbuffer_free(new_value);
    free(orig_value);
    free(error);
}

/* remove `value` strings from a string value */
void list_remove_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key, *remove_value, *orig_value, *token;
    size_t              orig_valuelen;
    char                sep;
    int                 format, ret_data;
    struct evbuffer     *new_value;
    struct evkeyvalq    args;
    struct evkeyval     *arg_pair;
    struct json_object  *jsobj = NULL, *jsobj_data = NULL, *jsobj_removed = NULL, *jsobj_value = NULL;
    struct HashValue    *remove_values_hash, *hash_value, *hash_tmp;
    struct ListInfo     list_info;
    int response_code = HTTP_OK;
    char *error = NULL;
    int updated = 0;
    leveldb_readoptions_t *read_options;
    leveldb_writeoptions_t *write_options;
    
    evhttp_parse_query(req->uri, &args);
    format = get_argument_format(&args);
    ret_data = get_int_argument(&args, "return_data", 0);
    
    key = (char *)evhttp_find_header(&args, "key");
    remove_value = (char *)evhttp_find_header(&args, "value");
    sep = get_argument_separator(&args);
    
    if (key == NULL) {
        finalize_request(400, "MISSING_ARG_KEY", req, evb, &args, jsobj);
        return;
    }
    if (remove_value == NULL) {
        finalize_request(400, "MISSING_ARG_VALUE", req, evb, &args, jsobj);
        return;
    }
    if (sep == 0) {
        finalize_request(400, "INVALID_SEPARATOR", req, evb, &args, jsobj);
        return;
    }
    
    if (format == json_format) {
        jsobj = json_object_new_object();
        jsobj_data = json_object_new_object();
        jsobj_removed = json_object_new_array();
        json_object_object_add(jsobj, "data", jsobj_data);
        json_object_object_add(jsobj_data, "key", json_object_new_string(key));
        json_object_object_add(jsobj_data, "removed", jsobj_removed);
        
        if (ret_data) {
            jsobj_value = json_object_new_array();
            json_object_object_add(jsobj_data, "value", jsobj_value);
        }
    }
    
    // put values to remove in a hash for quick lookup as list is walked
    remove_values_hash = NULL;
    TAILQ_FOREACH(arg_pair, &args, next) {
        if (strcmp(arg_pair->key, "value") != 0) {
            continue;
        }
        // skip empty values
        if (strlen(arg_pair->value) == 0) {
            continue;
        }
        HASH_FIND_STR(remove_values_hash, arg_pair->value, hash_value);
        if (!hash_value) {
            hash_value = calloc(1, sizeof(struct HashValue));
            hash_value->value = arg_pair->value;
            HASH_ADD_KEYPTR(hh, remove_values_hash, hash_value->value, strlen(hash_value->value), hash_value);
        }
        hash_value->count++;
    }
    
    new_value = evbuffer_new();
    evbuffer_add_printf(new_value, "%s", ""); // null terminate
    
    read_options = leveldb_readoptions_create();
    leveldb_readoptions_set_verify_checksums(read_options, option_get_int("verify_checksums"));
    orig_value = leveldb_get(ldb, read_options, key, strlen(key), &orig_valuelen, &error);
    leveldb_readoptions_destroy(read_options);
    
    if (orig_value && !error) {
        prepare_token_list(&list_info, &orig_value, orig_valuelen, sep);
        
        TOKEN_LIST_FOREACH(token, &list_info) {
            HASH_FIND_STR(remove_values_hash, token, hash_value);
            if (hash_value) {
                updated = 1;
                hash_value->count--;
                if (hash_value->count == 0) {
                    HASH_DEL(remove_values_hash, hash_value);
                    free(hash_value);
                }
                if (jsobj_removed) {
                    json_object_array_add(jsobj_removed, json_object_new_string(token));
                }
                // don't copy this item
            } else {
                serialize_list_item(new_value, token, sep);
                if (jsobj_value) {
                    json_object_array_add(jsobj_value, json_object_new_string(token));
                }
            }
        }
        
        if (updated == 1) {
            write_options = leveldb_writeoptions_create();
            leveldb_put(ldb, write_options, key, strlen(key), (char *)EVBUFFER_DATA(new_value), EVBUFFER_LENGTH(new_value), &error);
            leveldb_writeoptions_destroy(write_options);
        }
    }
    
    HASH_ITER(hh, remove_values_hash, hash_value, hash_tmp) {
        HASH_DEL(remove_values_hash, hash_value);
        free(hash_value);
    }
    
    if (ret_data && format == txt_format) {
        evbuffer_add_printf(evb, "%s%c%s\n", key, sep, (char *)EVBUFFER_DATA(new_value));
    }
    
    finalize_request(response_code, error, req, evb, &args, jsobj);
    evbuffer_free(new_value);
    free(orig_value);
    free(error);
}

/* pop `count` strings from a string representation of a list */
void list_pop_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key, *orig_value, *token, *origptr;
    size_t              orig_valuelen;
    char                sep;
    int                 position, count, cur_pos, format, ret_data;
    struct evbuffer     *new_value, *pop_value = NULL;
    struct evkeyvalq    args;
    struct json_object  *jsobj = NULL, *jsobj_data = NULL, *jsobj_popped = NULL, *jsobj_value = NULL;
    struct ListInfo     list_info;
    int response_code = HTTP_OK;
    char *error = NULL;
    int updated = 0;
    leveldb_readoptions_t *read_options;
    leveldb_writeoptions_t *write_options;
    
    evhttp_parse_query(req->uri, &args);
    format = get_argument_format(&args);
    ret_data = get_int_argument(&args, "return_data", 0);
    
    key = (char *)evhttp_find_header(&args, "key");
    position = get_int_argument(&args, "position", 0);
    count = get_int_argument(&args, "count", 1);
    sep = get_argument_separator(&args);
    
    if (key == NULL) {
        finalize_request(400, "MISSING_ARG_KEY", req, evb, &args, jsobj);
        return;
    }
    if (sep == 0) {
        finalize_request(400, "INVALID_SEPARATOR", req, evb, &args, jsobj);
        return;
    }
    if (count < 1) {
        finalize_request(400, "INVALID_COUNT", req, evb, &args, jsobj);
        return;
    }
    
    new_value = evbuffer_new();
    evbuffer_add_printf(new_value, "%s", ""); // null terminate
    
    if (format == json_format) {
        jsobj = json_object_new_object();
        jsobj_data = json_object_new_object();
        jsobj_popped = json_object_new_array();
        json_object_object_add(jsobj, "data", jsobj_data);
        json_object_object_add(jsobj_data, "key", json_object_new_string(key));
        json_object_object_add(jsobj_data, "popped", jsobj_popped);
        
        if (ret_data) {
            jsobj_value = json_object_new_array();
            json_object_object_add(jsobj_data, "value", jsobj_value);
        }
    } else {
        pop_value = evbuffer_new();
        evbuffer_add_printf(pop_value, "%s", ""); // null terminate
    }
    
    read_options = leveldb_readoptions_create();
    leveldb_readoptions_set_verify_checksums(read_options, option_get_int("verify_checksums"));
    orig_value = leveldb_get(ldb, read_options, key, strlen(key), &orig_valuelen, &error);
    leveldb_readoptions_destroy(read_options);
    
    if (orig_value && !error) {
        prepare_token_list(&list_info, &orig_value, orig_valuelen, sep);
        
        if (position >= 0) {
            cur_pos = 0;
            TOKEN_LIST_FOREACH(token, &list_info) {
                if (cur_pos == position && count > 0) {
                    if (format == json_format) {
                        json_object_array_add(jsobj_popped, json_object_new_string(token));
                    } else {
                        serialize_list_item(pop_value, token, sep);
                    }
                    count--;
                    updated = 1;
                } else {
                    if (jsobj_value) {
                        json_object_array_add(jsobj_value, json_object_new_string(token));
                    }
                    serialize_list_item(new_value, token, sep);
                    cur_pos++;
                }
            }
        } else {
            cur_pos = -1;
            TOKEN_LIST_FOREACH_REVERSE(token, &list_info) {
                if (cur_pos == position && count > 0) {
                    if (format == json_format) {
                        json_object_array_add(jsobj_popped, json_object_new_string(token));
                    } else {
                        serialize_list_item(pop_value, token, sep);
                    }
                    count--;
                    updated = 1;
                    // blank out, will not be in reconstituted list
                    memset(token, '\0', strlen(token));
                } else {
                    // will be in reconstituted list
                    cur_pos--;
                }
            }
            
            // reconstitute
            if (updated) {
                origptr = orig_value;
                while (origptr < orig_value + orig_valuelen) {
                    if (*origptr != '\0') {
                        if (jsobj_value) {
                            json_object_array_add(jsobj_value, json_object_new_string(origptr));
                        }
                        serialize_list_item(new_value, origptr, sep);
                        origptr += strlen(origptr);
                    }
                    origptr++;
                }
            }
        }
        
        if (updated == 1) {
            write_options = leveldb_writeoptions_create();
            leveldb_put(ldb, write_options, key, strlen(key), (char *)EVBUFFER_DATA(new_value), EVBUFFER_LENGTH(new_value), &error);
            leveldb_writeoptions_destroy(write_options);
        }
    }
    
    if (format == txt_format) {
        evbuffer_add_printf(evb, "%s\n", (char *)EVBUFFER_DATA(pop_value));
        evbuffer_free(pop_value);
    }
    
    finalize_request(response_code, error, req, evb, &args, jsobj);
    evbuffer_free(new_value);
    free(orig_value);
    free(error);
}

void set_add_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key, *add_value, *orig_value;
    size_t              orig_valuelen;
    char                sep;
    int                 format, ret_data;
    struct evbuffer     *new_value;
    struct evkeyvalq    args;
    struct evkeyval     *arg_pair;
    struct SetItem      *set = NULL, *set_item;
    struct json_object  *jsobj = NULL, *jsobj_data = NULL, *jsobj_added = NULL, *jsobj_value = NULL;
    int response_code = HTTP_OK;
    char *error = NULL;
    int updated = 0;
    leveldb_readoptions_t *read_options;
    leveldb_writeoptions_t *write_options;
    
    evhttp_parse_query(req->uri, &args);
    format = get_argument_format(&args);
    ret_data = get_int_argument(&args, "return_data", 0);
    
    key = (char *)evhttp_find_header(&args, "key");
    add_value = (char *)evhttp_find_header(&args, "value");
    sep = get_argument_separator(&args);
    
    if (key == NULL) {
        finalize_request(400, "MISSING_ARG_KEY", req, evb, &args, jsobj);
        return;
    }
    if (add_value == NULL) {
        finalize_request(400, "MISSING_ARG_VALUE", req, evb, &args, jsobj);
        return;
    }
    if (sep == 0) {
        finalize_request(400, "INVALID_SEPARATOR", req, evb, &args, jsobj);
        return;
    }
    
    if (format == json_format) {
        jsobj = json_object_new_object();
        jsobj_data = json_object_new_object();
        jsobj_added = json_object_new_array();
        json_object_object_add(jsobj, "data", jsobj_data);
        json_object_object_add(jsobj_data, "key", json_object_new_string(key));
        json_object_object_add(jsobj_data, "added", jsobj_added);
        
        if (ret_data) {
            jsobj_value = json_object_new_array();
            json_object_object_add(jsobj_data, "value", jsobj_value);
        }
    }
    
    read_options = leveldb_readoptions_create();
    leveldb_readoptions_set_verify_checksums(read_options, option_get_int("verify_checksums"));
    orig_value = leveldb_get(ldb, read_options, key, strlen(key), &orig_valuelen, &error);
    leveldb_readoptions_destroy(read_options);
    
    new_value = evbuffer_new();
    evbuffer_add_printf(new_value, "%s", ""); // null terminate
    
    if (!error) {
        if (orig_value) {
            deserialize_alloc_set(&set, &orig_value, orig_valuelen, sep);
        }
        
        TAILQ_FOREACH(arg_pair, &args, next) {
            if (strcmp(arg_pair->key, "value") != 0) {
                continue;
            }
            // skip empty values
            if (strlen(arg_pair->value) == 0) {
                continue;
            }
            
            HASH_FIND_STR(set, arg_pair->value, set_item);
            if (!set_item) {
                add_new_set_item(&set, arg_pair->value);
                updated = 1;
                
                if (jsobj_added) {
                    json_object_array_add(jsobj_added, json_object_new_string(arg_pair->value));
                }
            }
        }
        
        serialize_free_set(new_value, jsobj_value, &set, sep);
        
        if (updated) {
            write_options = leveldb_writeoptions_create();
            leveldb_put(ldb, write_options, key, strlen(key), (char *)EVBUFFER_DATA(new_value), EVBUFFER_LENGTH(new_value), &error);
            leveldb_writeoptions_destroy(write_options);
        }
    }
    
    if (ret_data && format == txt_format) {
        evbuffer_add_printf(evb, "%s%c%s\n", key, sep, EVBUFFER_DATA(new_value));
    }
    
    finalize_request(response_code, error, req, evb, &args, jsobj);
    evbuffer_free(new_value);
    free(orig_value);
    free(error);
}

void set_remove_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key, *remove_value, *orig_value;
    size_t              orig_valuelen;
    char                sep;
    int                 format, ret_data;
    struct evbuffer     *new_value;
    struct evkeyvalq    args;
    struct evkeyval     *arg_pair;
    struct SetItem      *set = NULL, *set_item;
    struct json_object  *jsobj = NULL, *jsobj_data = NULL, *jsobj_removed = NULL, *jsobj_value = NULL;
    int response_code = HTTP_OK;
    char *error = NULL;
    int updated = 0;
    leveldb_readoptions_t *read_options;
    leveldb_writeoptions_t *write_options;
    
    evhttp_parse_query(req->uri, &args);
    format = get_argument_format(&args);
    ret_data = get_int_argument(&args, "return_data", 0);
    
    key = (char *)evhttp_find_header(&args, "key");
    remove_value = (char *)evhttp_find_header(&args, "value");
    sep = get_argument_separator(&args);
    
    if (key == NULL) {
        finalize_request(400, "MISSING_ARG_KEY", req, evb, &args, jsobj);
        return;
    }
    if (remove_value == NULL) {
        finalize_request(400, "MISSING_ARG_VALUE", req, evb, &args, jsobj);
        return;
    }
    if (sep == 0) {
        finalize_request(400, "INVALID_SEPARATOR", req, evb, &args, jsobj);
        return;
    }
    
    if (format == json_format) {
        jsobj = json_object_new_object();
        jsobj_data = json_object_new_object();
        jsobj_removed = json_object_new_array();
        json_object_object_add(jsobj, "data", jsobj_data);
        json_object_object_add(jsobj_data, "key", json_object_new_string(key));
        json_object_object_add(jsobj_data, "removed", jsobj_removed);
        
        if (ret_data) {
            jsobj_value = json_object_new_array();
            json_object_object_add(jsobj_data, "value", jsobj_value);
        }
    }
    
    read_options = leveldb_readoptions_create();
    leveldb_readoptions_set_verify_checksums(read_options, option_get_int("verify_checksums"));
    orig_value = leveldb_get(ldb, read_options, key, strlen(key), &orig_valuelen, &error);
    leveldb_readoptions_destroy(read_options);
    
    new_value = evbuffer_new();
    evbuffer_add_printf(new_value, "%s", ""); // null terminate
    
    if (orig_value && !error) {
        deserialize_alloc_set(&set, &orig_value, orig_valuelen, sep);
        
        TAILQ_FOREACH(arg_pair, &args, next) {
            if (strcmp(arg_pair->key, "value") != 0) {
                continue;
            }
            // skip empty values
            if (strlen(arg_pair->value) == 0) {
                continue;
            }
            
            HASH_FIND_STR(set, arg_pair->value, set_item);
            if (set_item) {
                HASH_DEL(set, set_item);
                free(set_item);
                updated = 1;
                
                if (jsobj_removed) {
                    json_object_array_add(jsobj_removed, json_object_new_string(arg_pair->value));
                }
            }
        }
        
        serialize_free_set(new_value, jsobj_value, &set, sep);
        
        if (updated) {
            write_options = leveldb_writeoptions_create();
            leveldb_put(ldb, write_options, key, strlen(key), (char *)EVBUFFER_DATA(new_value), EVBUFFER_LENGTH(new_value), &error);
            leveldb_writeoptions_destroy(write_options);
        }
    }
    
    if (ret_data && format == txt_format) {
        evbuffer_add_printf(evb, "%s%c%s\n", key, sep, EVBUFFER_DATA(new_value));
    }
    
    finalize_request(response_code, error, req, evb, &args, jsobj);
    evbuffer_free(new_value);
    free(orig_value);
    free(error);
}

void set_pop_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key, *orig_value;
    size_t              orig_valuelen;
    char                sep;
    int                 format, count, ret_data;
    struct evkeyvalq    args;
    struct evbuffer     *new_value, *pop_value = NULL;
    struct SetItem      *set = NULL, *set_item, *set_tmp;
    struct json_object  *jsobj = NULL, *jsobj_data = NULL, *jsobj_popped = NULL, *jsobj_value = NULL;
    int response_code = HTTP_OK;
    char *error = NULL;
    int updated = 0;
    leveldb_readoptions_t *read_options;
    leveldb_writeoptions_t *write_options;
    
    evhttp_parse_query(req->uri, &args);
    format = get_argument_format(&args);
    ret_data = get_int_argument(&args, "return_data", 0);
    
    key = (char *)evhttp_find_header(&args, "key");
    count = get_int_argument(&args, "count", 1);
    sep = get_argument_separator(&args);
    
    if (key == NULL) {
        finalize_request(400, "MISSING_ARG_KEY", req, evb, &args, jsobj);
        return;
    }
    if (count < 1) {
        finalize_request(400, "INVALID_COUNT", req, evb, &args, jsobj);
        return;
    }
    if (sep == 0) {
        finalize_request(400, "INVALID_SEPARATOR", req, evb, &args, jsobj);
        return;
    }
    
    if (format == json_format) {
        jsobj = json_object_new_object();
        jsobj_data = json_object_new_object();
        jsobj_popped = json_object_new_array();
        json_object_object_add(jsobj, "data", jsobj_data);
        json_object_object_add(jsobj_data, "key", json_object_new_string(key));
        json_object_object_add(jsobj_data, "popped", jsobj_popped);
        
        if (ret_data) {
            jsobj_value = json_object_new_array();
            json_object_object_add(jsobj_data, "value", jsobj_value);
        }
    } else {
        pop_value = evbuffer_new();
        evbuffer_add_printf(pop_value, "%s", ""); // null terminate
    }
    
    read_options = leveldb_readoptions_create();
    leveldb_readoptions_set_verify_checksums(read_options, option_get_int("verify_checksums"));
    orig_value = leveldb_get(ldb, read_options, key, strlen(key), &orig_valuelen, &error);
    leveldb_readoptions_destroy(read_options);
    
    new_value = evbuffer_new();
    evbuffer_add_printf(new_value, "%s", ""); // null terminate
    
    if (orig_value && !error) {
        deserialize_alloc_set(&set, &orig_value, orig_valuelen, sep);
        
        HASH_ITER(hh, set, set_item, set_tmp) {
            if (count <= 0) {
                break;
            }
            if (format == json_format) {
                json_object_array_add(jsobj_popped, json_object_new_string(set_item->value));
            } else {
                serialize_list_item(pop_value, set_item->value, sep);
            }
            HASH_DEL(set, set_item);
            free(set_item);
            count--;
            updated = 1;
        }
        
        serialize_free_set(new_value, jsobj_value, &set, sep);
        
        if (updated) {
            write_options = leveldb_writeoptions_create();
            leveldb_put(ldb, write_options, key, strlen(key), (char *)EVBUFFER_DATA(new_value), EVBUFFER_LENGTH(new_value), &error);
            leveldb_writeoptions_destroy(write_options);
        }
    }
    
    if (format == txt_format) {
        evbuffer_add_printf(evb, "%s\n", EVBUFFER_DATA(pop_value));
        evbuffer_free(pop_value);
    }
    
    finalize_request(response_code, error, req, evb, &args, jsobj);
    evbuffer_free(new_value);
    free(orig_value);
    free(error);
}

/*
return a txt format'd csv (key,value\n) reponse based on a forward match of the keys
note: this makes a snapshot of the database and may return after other data has been added
*/
void dump_csv_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct evkeyvalq args;
    struct json_object *jsobj = NULL;
    
    evhttp_parse_query(req->uri, &args);
    dump_fwmatch_key = (char *)evhttp_find_header(&args, "key");
    if (dump_fwmatch_key) {
        dump_fwmatch_key = strdup(dump_fwmatch_key);
    }
    
    if (is_currently_dumping) {
        finalize_request(500, "ALREADY_DUMPING", req, evb, &args, jsobj);
        free(dump_fwmatch_key);
        return;
    }
    is_currently_dumping = 1;
    
    /* init the state for dumping data */
    dump_read_options = leveldb_readoptions_create();
    dump_snapshot = leveldb_create_snapshot(ldb);
    leveldb_readoptions_set_snapshot(dump_read_options, dump_snapshot);
    dump_iter = leveldb_create_iterator(ldb, dump_read_options);
    
    if (dump_fwmatch_key) {
        leveldb_iter_seek(dump_iter, dump_fwmatch_key, strlen(dump_fwmatch_key));
    } else {
        leveldb_iter_seek_to_first(dump_iter);
    }
    
    evhttp_clear_headers(&args);
    evhttp_send_reply_start(req, 200, "OK");
    evhttp_connection_set_closecb(req->evcon, cleanup_dump_csv_cb, NULL);
    
    /* run the first dump loop */
    do_dump_csv(0, 0, req);
}

void do_dump_csv(int fd, short what, void *ctx)
{
    struct evhttp_request *req = (struct evhttp_request *)ctx;
    struct evbuffer *evb;
    int c = 0, set_timer = 0, send_reply = 0;
    const char *key, *value;
    size_t key_len, value_len;
    struct timeval time_start, time_now;
    struct evhttp_connection *evcon;
    unsigned long output_buffer_length;
    
    gettimeofday(&time_start, NULL);
    evb = req->output_buffer;
    
    // if backed up, continue later
    evcon = (struct evhttp_connection *)req->evcon;
    output_buffer_length = evcon->output_buffer ? (unsigned long)EVBUFFER_LENGTH(evcon->output_buffer) : 0;
    if (output_buffer_length > DUMP_CSV_MAX_BUFFER) {
        set_dump_csv_timer(req);
        return;
    }
    
    while (leveldb_iter_valid(dump_iter)) {
        key = leveldb_iter_key(dump_iter, &key_len);
        if (dump_fwmatch_key) {
            // this is the case where we are only dumping keys of this prefix
            // so we need to break out of the loop at the last key
            if (strlen(dump_fwmatch_key) > key_len || strncmp(key, dump_fwmatch_key, strlen(dump_fwmatch_key)) != 0 ) {
                break;
            }
        }
        value = leveldb_iter_value(dump_iter, &value_len);
        evbuffer_add(evb, key, key_len);
        evbuffer_add(evb, ",", 1);
        evbuffer_add(evb, value, value_len);
        evbuffer_add(evb, "\n", 1);
        leveldb_iter_next(dump_iter);
        
        send_reply = 1;
        c++;
        if (c == DUMP_CSV_ITERS_CHECK) {
            int64_t usecs;
            gettimeofday(&time_now, NULL);
            usecs = 0 + ((int64_t)time_now  .tv_sec * 1000000 + time_now  .tv_usec)
                    -   ((int64_t)time_start.tv_sec * 1000000 + time_start.tv_usec);
            if (usecs > DUMP_CSV_MSECS_WORK * 1000) {
                set_timer = 1;
                break;
            }
            c = 0;
        }
    }
    
    // leveldb_iter_get_error(dump_iter, &err);
    
    if (send_reply) {
        evhttp_send_reply_chunk(req, evb);
    }
    
    if (set_timer) {
        set_dump_csv_timer(req);
    } else {
        evhttp_send_reply_end(req);
        // we're finished this request, but connection might be reused
        evhttp_connection_set_closecb(req->evcon, NULL, NULL);
        cleanup_dump_csv_cb(NULL, NULL);
    }
}

void set_dump_csv_timer(struct evhttp_request *req)
{
    struct timeval tv = {0, DUMP_CSV_MSECS_SLEEP * 1000};
    
    evtimer_del(&dump_ev);
    evtimer_set(&dump_ev, do_dump_csv, req);
    evtimer_add(&dump_ev, &tv);
}

void cleanup_dump_csv_cb(struct evhttp_connection *evcon, void *arg)
{
    evtimer_del(&dump_ev);
    leveldb_iter_destroy(dump_iter);
    leveldb_readoptions_destroy(dump_read_options);
    leveldb_release_snapshot(ldb, dump_snapshot);
    free(dump_fwmatch_key);
    is_currently_dumping = 0;
}


void stats_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    int i;
    struct evkeyvalq args;
    const char *format;
    
    struct simplehttp_stats *st;
    
    st = simplehttp_stats_new();
    simplehttp_stats_get(st);
    
    evhttp_parse_query(req->uri, &args);
    format = (char *)evhttp_find_header(&args, "format");
    
    if ((format != NULL) && (strcmp(format, "json") == 0)) {
        evbuffer_add_printf(evb, "{");
        for (i = 0; i < st->callback_count; i++) {
            evbuffer_add_printf(evb, "\"%s_95\": %"PRIu64",", st->stats_labels[i], st->ninety_five_percents[i]);
            evbuffer_add_printf(evb, "\"%s_average_request\": %"PRIu64",", st->stats_labels[i], st->average_requests[i]);
            evbuffer_add_printf(evb, "\"%s_requests\": %"PRIu64",", st->stats_labels[i], st->stats_counts[i]);
        }
        evbuffer_add_printf(evb, "\"total_requests\": %"PRIu64, st->requests);
        evbuffer_add_printf(evb, "}\n");
    } else {
        evbuffer_add_printf(evb, "total requests: %"PRIu64"\n", st->requests);
        for (i = 0; i < st->callback_count; i++) {
            evbuffer_add_printf(evb, "/%s 95%%: %"PRIu64"\n", st->stats_labels[i], st->ninety_five_percents[i]);
            evbuffer_add_printf(evb, "/%s average request (usec): %"PRIu64"\n", st->stats_labels[i], st->average_requests[i]);
            evbuffer_add_printf(evb, "/%s requests: %"PRIu64"\n", st->stats_labels[i], st->stats_counts[i]);
        }
    }
    
    simplehttp_stats_free(st);
    
    evhttp_send_reply(req, HTTP_OK, "OK", evb);
    evhttp_clear_headers(&args);
}

void exit_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    fprintf(stdout, "/exit request recieved\n");
    event_loopbreak();
}

void info()
{
    fprintf(stdout, "simpleleveldb: a light http interface to leveldb.\n");
    fprintf(stdout, "Version: %s, https://github.com/bitly/simplehttp/tree/master/simpleleveldb\n", VERSION);
}

int version_cb(int value)
{
    fprintf(stdout, "Version: %s\n", VERSION);
    return 0;
}

int main(int argc, char **argv)
{
    define_simplehttp_options();
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
    
    if (!option_parse_command_line(argc, argv)) {
        return 1;
    }
    
    info();
    
    if (!db_open()) {
        return 1;
    }
    
    simplehttp_init();
    simplehttp_set_cb("/list_append*", list_add_cb, (void *) LIST_APPEND);
    simplehttp_set_cb("/list_prepend*", list_add_cb, (void *) LIST_PREPEND);
    simplehttp_set_cb("/list_remove*", list_remove_cb, NULL);
    simplehttp_set_cb("/list_pop*", list_pop_cb, NULL);
    simplehttp_set_cb("/set_add*", set_add_cb, NULL);
    simplehttp_set_cb("/set_remove*", set_remove_cb, NULL);
    simplehttp_set_cb("/set_pop*", set_pop_cb, NULL);
    simplehttp_set_cb("/get*", get_cb, NULL);
    simplehttp_set_cb("/mget*", mget_cb, NULL);
    simplehttp_set_cb("/range_match*", range_match_cb, NULL);
    simplehttp_set_cb("/fwmatch*", fwmatch_cb, NULL);
    simplehttp_set_cb("/put*", put_cb, NULL);
    simplehttp_set_cb("/mput*", mput_cb, NULL);
    simplehttp_set_cb("/del*", del_cb, NULL);
    simplehttp_set_cb("/stats*", stats_cb, NULL);
    simplehttp_set_cb("/exit*", exit_cb, NULL);
    simplehttp_set_cb("/dump_csv*", dump_csv_cb, NULL);
    
    simplehttp_main();
    
    db_close();
    free_options();
    
    return 0;
}
