#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <json/json.h>
#include <simplehttp/options.h>
#include "md5.h"

#define VERSION "1.0"

#define BUF_SZ (1024 * 16)

char *md5_hash(const char *string);
int parse_encrypted_fields(char *str);
int parse_blacklisted_fields(char *str);
int parse_fields(const char *str, char **field_array);

static char *encrypted_fields[64];
static int  num_encrypted_fields = 0;
static char *blacklisted_fields[64];
static int  num_blacklisted_fields = 0;
static char *expected_key = NULL;
static char *expected_value = NULL;

static char buf[BUF_SZ];

/*
 * Parse a comma-delimited  string and populate
 * the blacklisted_fields array with the results.
 *
 * See parse_fields().
 */
int parse_blacklisted_fields(char *str)
{
    int i;
    
    num_blacklisted_fields = parse_fields(str, blacklisted_fields);
    
    for (i = 0; i < num_blacklisted_fields; i++) {
        fprintf(stderr, "Blacklist field: \"%s\"\n", blacklisted_fields[i]);
    }
    
    return 1;
}

/*
 * Parse a comma-delimited  string and populate
 * the encrypted_fields array with the results.
 *
 * See parse_fields().
 */
int parse_encrypted_fields(char *str)
{
    int i;
    num_encrypted_fields = parse_fields(str, encrypted_fields);
    
    for (i = 0; i < num_encrypted_fields; i++) {
        fprintf(stderr, "Encrypted field: \"%s\"\n", encrypted_fields[i]);
    }
    
    return 1;
}

/*
 * Parse a comma-delimited list of strings and put them
 * in an char array. Array better have enough slots
 * because I didn't have time to work out the memory allocation.
 */
int parse_fields(const char *str, char **field_array)
{
    int i;
    const char delim[] = ",";
    char *tok, *str_ptr, *save_ptr;
    
    if (!str) {
        return;
    }
    
    str_ptr = strdup(str);
    
    tok = strtok_r(str_ptr, delim, &save_ptr);
    
    i = 0;
    while (tok != NULL) {
        field_array[i] = strdup(tok);
        tok = strtok_r(NULL, delim, &save_ptr);
        i++;
    }
    
    return i;
}

/* md5 encrypt a string */
char *md5_hash(const char *string)
{
    char *output = calloc(1, 33 * sizeof(char));
    struct cvs_MD5Context context;
    unsigned char checksum[16];
    int i;
    
    cvs_MD5Init (&context);
    cvs_MD5Update (&context, string, strlen(string));
    cvs_MD5Final (checksum, &context);
    for (i = 0; i < 16; i++) {
        sprintf(&output[i * 2], "%02x", (unsigned int) checksum[i]);
    }
    output[32] = '\0';
    return output;
}

/*
 * for each message.
 */
void process_message(const char *source)
{
    struct json_object *json_in, *element;
    const char *field_key, *raw_string, *json_out;
    char *encrypted_string;
    int i;
    
    json_in = json_tokener_parse(source);
    
    if (json_in == NULL) {
        fprintf(stderr, "ERR: unable to parse json %s\n", source);
        return;
    }
    
    // filter messages
    if (expected_value != NULL) {
        element = json_object_object_get(json_in, expected_key);
        if (element == NULL) {
            json_object_put(json_in);
            return;
        }
        if (json_object_is_type(element, json_type_null)) {
            json_object_put(json_in);
            return;
        }
        raw_string = json_object_get_string(element);
        if (raw_string == NULL || !strlen(raw_string) || strcmp(raw_string, expected_value) != 0) {
            json_object_put(json_in);
            return;
        }
    }
    
    // loop through the fields we need to encrypt
    for (i = 0; i < num_encrypted_fields; i++) {
        field_key = encrypted_fields[i];
        element = json_object_object_get(json_in, field_key);
        if (element) {
            raw_string = json_object_get_string(element);
        } else {
            continue;
        }
        encrypted_string = md5_hash(raw_string);
        json_object_object_add(json_in, field_key, json_object_new_string(encrypted_string));
        free(encrypted_string);
    }
    
    // loop through and remove the blacklisted fields
    for (i = 0; i < num_blacklisted_fields; i++) {
        field_key = blacklisted_fields[i];
        json_object_object_del(json_in, field_key);
    }
    
    json_out = json_object_to_json_string(json_in);
    
    /* new line terminated */
    printf("%s\n", json_out);
    
    json_object_put(json_in);
}

int version_cb(int value)
{
    fprintf(stderr, "Version: %s\n", VERSION);
    return 0;
}

int main(int argc, char **argv)
{
    uint64_t msgRecv = 0;
    
    option_define_bool("version", OPT_OPTIONAL, 0, NULL, version_cb, VERSION);
    option_define_str("blacklist_fields", OPT_OPTIONAL, NULL, NULL, parse_blacklisted_fields, "comma separated list of fields to remove");
    option_define_str("encrypted_fields", OPT_OPTIONAL, NULL, NULL, parse_encrypted_fields, "comma separated list of fields to encrypt");
    option_define_str("expected_key", OPT_OPTIONAL, NULL, &expected_key, NULL, "key to expect in messages before echoing to clients");
    option_define_str("expected_value", OPT_OPTIONAL, NULL, &expected_value, NULL, "value to expect in --expected-key field in messages before echoing to clients");
    
    if (!option_parse_command_line(argc, argv)) {
        return 1;
    }
    
    if ( !!expected_key ^ !!expected_value ) {
        fprintf(stderr, "--expected-key and --expected-value must be used together\n");
        exit(1);
    }
    
    while (fgets(buf, BUF_SZ, stdin)) {
        process_message(buf);
        
        msgRecv++;
        if (msgRecv % 5000) {
            fprintf(stderr, "message #%lu\r", msgRecv);
        }
    }
    
    free_options();
    
    return 0;
}
