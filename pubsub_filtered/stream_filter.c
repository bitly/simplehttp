#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <json/json.h>
#include <simplehttp/options.h>

#include "shared.h"

#define VERSION "1.2"

#define BUF_SZ (1024 * 16)
#define MAX_FIELDS 64

int parse_encrypted_fields(char *str);
int parse_blacklisted_fields(char *str);

static char *encrypted_fields[MAX_FIELDS];
static int  num_encrypted_fields = 0;
static char *blacklisted_fields[MAX_FIELDS];
static int  num_blacklisted_fields = 0;
static char *expected_key = NULL;
static char *expected_value = NULL;
static int verbose;

static uint64_t msgRecv = 0;
static uint64_t msgFail = 0;

static char buf[BUF_SZ];

/*
 * populate the blacklisted_fields array
 */
int parse_blacklisted_fields(char *str)
{
    num_blacklisted_fields = parse_fields(str, blacklisted_fields, MAX_FIELDS, "Blacklist", stderr);
    return (num_blacklisted_fields <= MAX_FIELDS);
}

/*
 * populate the encrypted_fields array with the results.
 */
int parse_encrypted_fields(char *str)
{
    num_encrypted_fields = parse_fields(str, encrypted_fields, MAX_FIELDS, "Encrypted", stderr);
    return (num_encrypted_fields <= MAX_FIELDS);
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
        msgFail++;
        if (verbose) {
            fprintf(stderr, "ERR: unable to parse json '%s'\n", source);
        }
        return;
    }
    
    // filter messages
    if (expected_value && !filter_message_simple(expected_key, expected_value, json_in)) {
        json_object_put(json_in);
        return;
    }
    
    // remove the blacklisted fields
    delete_fields(blacklisted_fields, num_blacklisted_fields, json_in);
    
    // fields we need to encrypt
    encrypt_fields(encrypted_fields, num_encrypted_fields, json_in);
    
    json_out = json_object_to_json_string(json_in);
    
    /* new line terminated */
    printf("%s\n", json_out);
    
    json_object_put(json_in);
}

int version_cb(int value)
{
    fprintf(stderr, "Version: %s\n", VERSION);
    exit(0);
}

int main(int argc, char **argv)
{
    option_define_bool("version", OPT_OPTIONAL, 0, NULL, version_cb, VERSION);
    option_define_bool("verbose", OPT_OPTIONAL, 0, &verbose, NULL, "verbose output (to stderr)");
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
        msgRecv++;
        process_message(buf);
    }
    
    fprintf(stderr, "processed %lu lines, failed to parse %lu of them\n", msgRecv, msgFail);
    free_options();
    free_fields(blacklisted_fields, num_blacklisted_fields);
    free_fields(encrypted_fields, num_encrypted_fields);
    
    return 0;
}
