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

#define NAME            "simpleleveldb"
#define VERSION         "0.1"

void finalize_request(int response_code, char *error, struct evhttp_request *req, struct evbuffer *evb, struct evkeyvalq *args, struct json_object *jsobj);
int db_open();
void db_close();
void del_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void put_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void get_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void mget_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void stats_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void exit_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void list_append_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);

leveldb_t *ldb;
leveldb_options_t *ldb_options;
leveldb_cache_t *ldb_cache;


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
    ldb_cache = leveldb_cache_create_lru(option_get_int("block_size"));
    
    leveldb_options_set_create_if_missing(ldb_options, option_get_int("create_db_if_missing"));
    leveldb_options_set_error_if_exists(ldb_options, option_get_int("error_if_db_exists"));
    leveldb_options_set_paranoid_checks(ldb_options, option_get_int("paranoid_checks"));
    leveldb_options_set_write_buffer_size(ldb_options, option_get_int("write_buffer_size"));
    leveldb_options_set_block_size(ldb_options, option_get_int("write_buffer_size"));
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

void put_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char                *key, *value;
    struct evkeyvalq    args;
    struct json_object  *jsobj;
    int response_code = HTTP_OK;
    char *error = NULL;
    leveldb_writeoptions_t *write_options;
    
    evhttp_parse_query(req->uri, &args);
    
    key = (char *)evhttp_find_header(&args, "key");
    value = (char *)evhttp_find_header(&args, "value");
    
    jsobj = json_object_new_object();
    if (key == NULL) {
        finalize_request(400, "MISSING_ARG_KEY", req, evb, &args, jsobj);
        return;
    }
    
    if (value == NULL) {
        finalize_request(400, "MISSING_ARG_VALUE", req, evb, &args, jsobj);
        return;
    }
    
    write_options = leveldb_writeoptions_create();
    leveldb_put(ldb, write_options, key, strlen(key), value, strlen(value), &error);
    leveldb_writeoptions_destroy(write_options);
    
    if (!error) {
        json_object_object_add(jsobj, "data", json_object_new_string(value));
    }
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

    TAILQ_FOREACH(pair, &args, next) {
        if (pair->key[0] != 'k') continue;
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
    leveldb_readoptions_t *read_options;
    leveldb_writeoptions_t *write_options;

    evhttp_parse_query(req->uri, &args);
    format = get_argument_format(&args);
    
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
        new_value = calloc(orig_valuelen + 1 + strlen(append_value) + 1, sizeof(char *));
        sprintf(new_value, "%s,%s", orig_value, append_value);
    } else {
        new_value = calloc(strlen(append_value) + 1, sizeof(char *));
        sprintf(new_value, "%s", append_value);
    }

    free(error);

    write_options = leveldb_writeoptions_create();
    leveldb_put(ldb, write_options, key, strlen(key), new_value, strlen(new_value), &error);
    leveldb_writeoptions_destroy(write_options);

    if (format == json_format) {
        json_object_object_add(jsobj, "data", json_object_new_string(new_value));
    } else {
        evbuffer_add_printf(evb, "%s,%s\n", key, new_value);
    }

    finalize_request(response_code, error, req, evb, &args, jsobj);
    free(new_value);
    free(orig_value);
    free(error);
    
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

int version_cb(int value) {
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
    option_define_int("write_buffer_size", OPT_OPTIONAL, 4<<20, NULL, NULL, "write buffer size");
    option_define_int("block_size", OPT_OPTIONAL, 4096, NULL, NULL, "block size");
    option_define_bool("compression", OPT_OPTIONAL, 1, NULL, NULL, "snappy compression");
    option_define_int("leveldb_max_open_files", OPT_OPTIONAL, 4096, NULL, NULL, "leveldb max open files");
    
    if (!option_parse_command_line(argc, argv)){
        return 1;
    }
    
    info();
    
    if (!db_open()) {
        return 1;
    }
    
    simplehttp_init();
    simplehttp_set_cb("/list_append*", list_append_cb, NULL);
    simplehttp_set_cb("/get*", get_cb, NULL);
    simplehttp_set_cb("/mget*", mget_cb, NULL);
    simplehttp_set_cb("/put*", put_cb, NULL);
    simplehttp_set_cb("/del*", del_cb, NULL);
    simplehttp_set_cb("/stats*", stats_cb, NULL);
    simplehttp_set_cb("/exit*", exit_cb, NULL);
    simplehttp_main();
    
    db_close();
    free_options();
    
    return 0;
}
