#define _GNU_SOURCE
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <inttypes.h>
#include <simplehttp/queue.h>
#include <simplehttp/simplehttp.h>
#include <json/json.h>
#include <leveldb/c.h>

#include <sys/socket.h>
#include "http-internal.h"

#define NAME            "simpleleveldb"
#define VERSION         "0.7"

#define DUMP_CSV_ITERS_CHECK       10
#define DUMP_CSV_MSECS_WORK        10
#define DUMP_CSV_MSECS_SLEEP      100
#define DUMP_CSV_MAX_BUFFER        (8*1024*1024)

void finalize_request(int response_code, char *error, struct evhttp_request *req, struct evbuffer *evb, struct evkeyvalq *args, struct json_object *jsobj);
int db_open();
void db_close();
void del_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void put_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void mput_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void get_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void mget_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void fwmatch_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void range_match_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void stats_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void exit_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void list_append_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void list_remove_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void dump_csv_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void do_dump_csv(int fd, short what, void *ctx);
void set_dump_csv_timer(struct evhttp_request *req);
void cleanup_dump_csv_cb(struct evhttp_connection *evcon, void *arg);

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
char *MPUT_SEP = ",";

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


void del_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key;
    struct evkeyvalq    args;
    struct json_object  *jsobj;
    int response_code = HTTP_OK;
    char *error = NULL;
    leveldb_writeoptions_t *write_options;
    
    evhttp_parse_query(req->uri, &args);
    
    jsobj = json_object_new_object();
    
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
    char                *term_key, *term_value, *sep;
    struct evkeyvalq    args;
    struct json_object  *jsobj;
    int response_code = HTTP_OK;
    char *error = NULL;
    size_t req_len, sep_pos = 0, line_offset = 0, j, sep_len;
    leveldb_writeoptions_t *write_options;
    
    evhttp_parse_query(req->uri, &args);
    jsobj = json_object_new_object();

    sep = (char*)evhttp_find_header(&args, "separator");
    if (sep == NULL) {
        sep = MPUT_SEP;
    }
    sep_len = strlen(sep);

    req_len = EVBUFFER_LENGTH(req->input_buffer);
    if (req->type != EVHTTP_REQ_POST) {
        finalize_request(400, "MUST_POST_DATA", req, evb, &args, jsobj);
        return;
    } else if (req_len <= 2) {
        finalize_request(400, "MISSING_ARG_VALUE", req, evb, &args, jsobj);
        return;
    }

    write_options = leveldb_writeoptions_create();
    for (j = 0; j <= req_len; j++) {
        if ( strncmp((char*)(EVBUFFER_DATA(req->input_buffer) + j), sep, sep_len) == 0 ) {
            sep_pos = j;
            j += sep_len - 1;
        } else if (j == req_len || *(EVBUFFER_DATA(req->input_buffer) + j) ==  '\n') {
            if (line_offset == j) {
                // Do nothing... just skip this blank line
            } else if (sep_pos == 0) {
                response_code = 400;
                error = "MALFORMED_CSV";
                break; // everything.
            } else {
                term_key = strndup((const char*)(EVBUFFER_DATA(req->input_buffer)+line_offset), sep_pos - line_offset);
                term_value = strndup((const char*)(EVBUFFER_DATA(req->input_buffer)+sep_pos+sep_len), j - (sep_pos+sep_len));

                leveldb_put(ldb, write_options, term_key, strlen(term_key), term_value, strlen(term_value), &error);

                free(term_key);
                free(term_value);
            }
            line_offset = j+1;
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
    struct json_object  *jsobj;
    int response_code = HTTP_OK;
    char *error = NULL;
    size_t value_len;
    leveldb_writeoptions_t *write_options;
    
    evhttp_parse_query(req->uri, &args);
    
    key = (char *)evhttp_find_header(&args, "key");
    
    
    jsobj = json_object_new_object();
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
    char                *key, *value, *terminated_value;
    struct evkeyvalq    args;
    struct json_object  *jsobj;
    int response_code = HTTP_OK;
    leveldb_readoptions_t *read_options;
    char *error = NULL;
    size_t vallen;
    char *tmp;
    int format;
    evhttp_parse_query(req->uri, &args);
    format = get_argument_format(&args);
    
    key = (char *)evhttp_find_header(&args, "key");
    
    jsobj = json_object_new_object();
    if (key == NULL) {
        finalize_request(400, "MISSING_ARG_KEY", req, evb, &args, jsobj);
        return;
    }
    
    read_options = leveldb_readoptions_create();
    leveldb_readoptions_set_verify_checksums(read_options, option_get_int("verify_checksums"));
    value = leveldb_get(ldb, read_options, key, strlen(key), &vallen, &error);
    leveldb_readoptions_destroy(read_options);
    
    if (value) {
        terminated_value = value;
        DUPE_N_TERMINATE(terminated_value, vallen, tmp);
        if (format == txt_format) {
            evbuffer_add_printf(evb, "%s,%s\n", key, terminated_value);
        } else {
            json_object_object_add(jsobj, "data", json_object_new_string(terminated_value));
        }
        free(terminated_value);
        
        finalize_request(response_code, error, req, evb, &args, jsobj);
    } else {
        finalize_request(404, "NOT_FOUND", req, evb, &args, jsobj);
    }
    free(value);
    free(error);
    
}

void mget_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key, *value, *terminated_value;
    int                 format;
    struct evkeyvalq    args;
    struct evkeyval     *pair;
    struct json_object  *jsobj = NULL;
    int nkeys = 0;
    int response_code = HTTP_OK;
    size_t vallen;
    char *error = NULL;
    char *tmp;
    leveldb_readoptions_t *read_options;
    
    evhttp_parse_query(req->uri, &args);
    format = get_argument_format(&args);
    
    if (format == json_format) {
        jsobj = json_object_new_object();
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
            terminated_value = value;
            DUPE_N_TERMINATE(terminated_value, vallen, tmp);
            if (format == json_format) {
                json_object_object_add(jsobj, key, json_object_new_string(terminated_value));
            } else {
                evbuffer_add_printf(evb, "%s,%s\n", key, terminated_value);
            }
            free(terminated_value);
        }
        free(value);
    }
    
    leveldb_readoptions_destroy(read_options);
    
    if (!nkeys) {
        finalize_request(400, "key is required", req, evb, &args, jsobj);
        return;
    }
    
    finalize_request(response_code, error, req, evb, &args, jsobj);
    free(error);
}

void range_match_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char *start_key, *end_key, *key_clean, *value_clean, *tmp;
    const char *key, *value;
    size_t key_len, value_len;
    struct evkeyvalq args;
    struct json_object *jsobj, *tmp_obj, *result_array;
    const leveldb_snapshot_t *bt_snapshot;
    leveldb_readoptions_t *bt_read_options;
    leveldb_iterator_t *bt_iter;
    int result_count = 0, result_limit = 0;
    
    evhttp_parse_query(req->uri, &args);
    start_key = (char *)evhttp_find_header(&args, "start");
    end_key = (char *)evhttp_find_header(&args, "end");
    result_limit = get_int_argument(&args, "limit", 500);
    
    jsobj = json_object_new_object();
    if (start_key == NULL || end_key == NULL) {
        finalize_request(400, "MISSING_ARG_KEY", req, evb, &args, jsobj);
        return;
    }
    if (strcmp(start_key, end_key) > 0) {
        finalize_request(400, "INVALID_START_KEY", req, evb, &args, jsobj);
        return;
    }

    result_array = json_object_new_array();
    tmp_obj = NULL;
    
    bt_read_options = leveldb_readoptions_create();
    bt_snapshot = leveldb_create_snapshot(ldb);
    leveldb_readoptions_set_snapshot(bt_read_options, bt_snapshot);
    bt_iter = leveldb_create_iterator(ldb, bt_read_options);
    
    leveldb_iter_seek(bt_iter, start_key, strlen(start_key));
    
    while (leveldb_iter_valid(bt_iter) && (result_limit == 0 || result_count < result_limit)) {
        key = leveldb_iter_key(bt_iter, &key_len);
        key_clean = (char *)key;
        DUPE_N_TERMINATE(key_clean, key_len, tmp);

        if (strcmp(key_clean, end_key) > 0) {
            free(key_clean);
            break;
        }

        value = leveldb_iter_value(bt_iter, &value_len);
        value_clean = (char *)value;
        DUPE_N_TERMINATE(value_clean, value_len, tmp);
        
        tmp_obj = json_object_new_object();
        json_object_object_add(tmp_obj, key_clean, json_object_new_string(value_clean));
        json_object_array_add(result_array, tmp_obj);
        
        leveldb_iter_next(bt_iter);
        result_count ++;
        
        free(key_clean);
        free(value_clean);
    }
    json_object_object_add(jsobj, "data", result_array);
    json_object_object_add(jsobj, "status", json_object_new_string(result_count ? "ok" : "no results"));
    
    finalize_request(200, NULL, req, evb, &args, jsobj);
    
    leveldb_iter_destroy(bt_iter);
    leveldb_readoptions_destroy(bt_read_options);
    leveldb_release_snapshot(ldb, bt_snapshot);
}

void fwmatch_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char *fw_key, *key_clean, *value_clean, *tmp;
    const char *key, *value;
    size_t key_len, value_len;
    struct evkeyvalq args;
    struct json_object *jsobj, *tmp_obj, *result_array;
    const leveldb_snapshot_t *fw_snapshot;
    leveldb_readoptions_t *fw_read_options;
    leveldb_iterator_t *fw_iter;
    int result_count = 0, result_limit = 0;
    
    evhttp_parse_query(req->uri, &args);
    fw_key = (char *)evhttp_find_header(&args, "key");
    result_limit = get_int_argument(&args, "limit", 500);
    
    jsobj = json_object_new_object();
    result_array = json_object_new_array();
    tmp_obj = NULL;
    
    if (fw_key == NULL) {
        finalize_request(400, "MISSING_ARG_KEY", req, evb, &args, jsobj);
        return;
    }
    
    fw_read_options = leveldb_readoptions_create();
    fw_snapshot = leveldb_create_snapshot(ldb);
    leveldb_readoptions_set_snapshot(fw_read_options, fw_snapshot);
    fw_iter = leveldb_create_iterator(ldb, fw_read_options);
    
    leveldb_iter_seek(fw_iter, fw_key, strlen(fw_key));
    
    while (leveldb_iter_valid(fw_iter) && (result_limit == 0 || result_count < result_limit)) {
        key = leveldb_iter_key(fw_iter, &key_len);
        key_clean = (char *)key;
        DUPE_N_TERMINATE(key_clean, key_len, tmp);
        
        // this is the case where we are only fwing keys of this prefix
        // so we need to break out of the loop at the last key
        if (strlen(fw_key) > key_len || strncmp(key_clean, fw_key, strlen(fw_key)) != 0 ) {
            free(key_clean);
            break;
        }
        value = leveldb_iter_value(fw_iter, &value_len);
        value_clean = (char *)value;
        DUPE_N_TERMINATE(value_clean, value_len, tmp);
        
        tmp_obj = json_object_new_object();
        json_object_object_add(tmp_obj, key_clean, json_object_new_string(value_clean));
        json_object_array_add(result_array, tmp_obj);
        
        leveldb_iter_next(fw_iter);
        result_count ++;
        
        free(key_clean);
        free(value_clean);
    }
    json_object_object_add(jsobj, "data", result_array);
    json_object_object_add(jsobj, "status", json_object_new_string(result_count ? "ok" : "no results"));
    
    finalize_request(200, NULL, req, evb, &args, jsobj);
    
    leveldb_iter_destroy(fw_iter);
    leveldb_readoptions_destroy(fw_read_options);
    leveldb_release_snapshot(ldb, fw_snapshot);
}

/* append a `value` string on to the end of a string value */
void list_append_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key, *append_value, *orig_value, *new_value, *terminated_value;
    struct evkeyvalq    args;
    struct json_object  *jsobj;
    int response_code = HTTP_OK;
    char *error = NULL;
    size_t orig_valuelen;
    char *tmp;
    int format;
    int echo_data;
    leveldb_readoptions_t *read_options;
    leveldb_writeoptions_t *write_options;
    
    evhttp_parse_query(req->uri, &args);
    format = get_argument_format(&args);
    echo_data = get_int_argument(&args, "echo_data", 0);
    
    key = (char *)evhttp_find_header(&args, "key");
    append_value = (char *)evhttp_find_header(&args, "value");
    // separator = (char *)evhttp_find_header(&args, "separator");
    
    jsobj = json_object_new_object();
    if (key == NULL) {
        finalize_request(400, "MISSING_ARG_KEY", req, evb, &args, jsobj);
        return;
    }
    if (append_value == NULL) {
        finalize_request(400, "MISSING_ARG_VALUE", req, evb, &args, jsobj);
        return;
    }
    
    read_options = leveldb_readoptions_create();
    leveldb_readoptions_set_verify_checksums(read_options, option_get_int("verify_checksums"));
    orig_value = leveldb_get(ldb, read_options, key, strlen(key), &orig_valuelen, &error);
    leveldb_readoptions_destroy(read_options);
    
    // null terminate orig_value
    if (orig_value) {
        terminated_value = orig_value;
        DUPE_N_TERMINATE(terminated_value, orig_valuelen, tmp);
        free(orig_value);
        orig_value = terminated_value;
    }
    
    
    if (orig_value) {
        new_value = calloc(1, (orig_valuelen + 1 + strlen(append_value) + 1) * sizeof(char *));
        sprintf(new_value, "%s,%s", orig_value, append_value);
    } else {
        new_value = calloc(1, (strlen(append_value) + 1) * sizeof(char *));
        sprintf(new_value, "%s", append_value);
    }
    
    free(error);
    
    write_options = leveldb_writeoptions_create();
    leveldb_put(ldb, write_options, key, strlen(key), new_value, strlen(new_value), &error);
    leveldb_writeoptions_destroy(write_options);
    
    if (echo_data) {
        if (format == json_format) {
            json_object_object_add(jsobj, "data", json_object_new_string(new_value));
        } else {
            evbuffer_add_printf(evb, "%s,%s\n", key, new_value);
        }
    }
    
    finalize_request(response_code, error, req, evb, &args, jsobj);
    free(new_value);
    free(orig_value);
    free(error);
    
}

/* remove a `value` from string */
void list_remove_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key, *remove_value, *orig_value, *terminated_value;
    struct evkeyvalq    args;
    struct json_object  *jsobj;
    int response_code = HTTP_OK;
    char *error = NULL;
    size_t orig_valuelen;
    char *tmp;
    int i;
    int updated = 0;
    char *token;
    struct evbuffer *new_value;
    int format;
    int echo_data;
    leveldb_readoptions_t *read_options;
    leveldb_writeoptions_t *write_options;
    
    evhttp_parse_query(req->uri, &args);
    format = get_argument_format(&args);
    echo_data = get_int_argument(&args, "echo_data", 0);
    
    key = (char *)evhttp_find_header(&args, "key");
    remove_value = (char *)evhttp_find_header(&args, "value");
    // separator = (char *)evhttp_find_header(&args, "separator");
    
    jsobj = json_object_new_object();
    if (key == NULL) {
        finalize_request(400, "MISSING_ARG_KEY", req, evb, &args, jsobj);
        return;
    }
    if (remove_value == NULL) {
        finalize_request(400, "MISSING_ARG_VALUE", req, evb, &args, jsobj);
        return;
    }
    
    read_options = leveldb_readoptions_create();
    leveldb_readoptions_set_verify_checksums(read_options, option_get_int("verify_checksums"));
    orig_value = leveldb_get(ldb, read_options, key, strlen(key), &orig_valuelen, &error);
    leveldb_readoptions_destroy(read_options);
    
    // null terminate orig_value
    if (orig_value) {
        terminated_value = orig_value;
        DUPE_N_TERMINATE(terminated_value, orig_valuelen, tmp);
        free(orig_value);
        orig_value = terminated_value;
    }
    
    if (orig_value) {
        new_value = evbuffer_new();
        token = strtok(orig_value, ",");
        i = 0;
        while (token) {
            if (strcmp(token, remove_value) == 0) {
                // we found the token
                updated = 1;
            } else {
                if (i == 0) {
                    evbuffer_add_printf(new_value, "%s", token);
                } else {
                    evbuffer_add_printf(new_value, ",%s", token);
                }
                i++;
            }
            token = strtok(NULL, ",");
        }
        if (updated == 1) {
            write_options = leveldb_writeoptions_create();
            leveldb_put(ldb, write_options, key, strlen(key), (char *)EVBUFFER_DATA(new_value), EVBUFFER_LENGTH(new_value), &error);
            leveldb_writeoptions_destroy(write_options);
        }
        
        if (echo_data) {
            if (format == json_format) {
                json_object_object_add(jsobj, "data", json_object_new_string((char *)EVBUFFER_DATA(new_value)));
            } else {
                evbuffer_add_printf(evb, "%s,%s\n", key, (char *)EVBUFFER_DATA(new_value));
            }
        }
        
        evbuffer_free(new_value);
        
    } else {
        if (echo_data) {
            if (format == json_format) {
                json_object_object_add(jsobj, "data", json_object_new_string(""));
            } else {
                evbuffer_add_printf(evb, "%s,\n", key);
            }
        }
    }
    
    free(error);
    
    finalize_request(response_code, error, req, evb, &args, jsobj);
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
    int format;
    struct json_object  *jsobj;
    
    jsobj = json_object_new_object();
    
    evhttp_parse_query(req->uri, &args);
    format = get_argument_format(&args);
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
    json_object_put(jsobj);
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
    simplehttp_set_cb("/list_append*", list_append_cb, NULL);
    simplehttp_set_cb("/list_remove*", list_remove_cb, NULL);
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
