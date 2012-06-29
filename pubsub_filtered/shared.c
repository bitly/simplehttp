#include "shared.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <json/json.h>

#include "md5.h"

/*
 * Parse a comma-delimited list of strings and put copies of them
 * in an array of (char *) with a specified max capacity.
 *
 * type and log are optional
 *
 * returns the number of fields present, even if more than could
 *         be stored in the array, but does not overflow the array
 */
int parse_fields(const char *str, char **field_array, int array_len, const char *type, FILE *log)
{
    int cnt;
    const char delim[] = ",";
    char *tok, *str_ptr, *save_ptr;
    
    if (!str) {
        return;
    }
    str_ptr = strdup(str);
    
    cnt = 0;
    for (tok = strtok_r(str_ptr, delim, &save_ptr);
         tok != NULL;
         tok = strtok_r(NULL, delim, &save_ptr)    ) {
        if (log) {
            fprintf(log, "%s field: \"%s\"\n", type ? type : "parsed", tok);
        }
        if (cnt < array_len) {
            field_array[cnt] = strdup(tok);
        }
        cnt++;
    }
    
    if (cnt > array_len) {
        fprintf(stderr, "Exceeded maximum of %d %s fields\n",
                        array_len, type ? type : "parsed"    );
    }
    free(str_ptr);
    return cnt;
}

void free_fields(char **field_array, int array_len)
{
    int i;
    
    for (i = 0; i < array_len; i++) {
        free(field_array[i]);
    }
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
 * Filters a JSON object based on simple key/value
 * return non-zero if message should be kept
 */
int filter_message_simple(const char *key, const char *value, struct json_object *msg)
{
    const char *raw_string;
    json_object *element;
    
    element = json_object_object_get(msg, key);
    if (element == NULL) {
        return 0;
    }
    if (json_object_is_type(element, json_type_null)) {
        return 0;
    }
    raw_string = json_object_get_string(element);
    if (raw_string == NULL || !strlen(raw_string) || strcmp(raw_string, value) != 0) {
        return 0;
    }
    return 1;
}

/*
 * delete fields from a json message
 */
void delete_fields(char **fields, int len, struct json_object *msg)
{
    const char *field_key;
    int i;
    
    for (i = 0; i < len; i++) {
        field_key = fields[i];
        json_object_object_del(msg, field_key);
    }
}

/*
 * encrypt fields in a json message
 */
void encrypt_fields(char **fields, int len, struct json_object *msg)
{
    const char *field_key, *raw_string;
    char *encrypted_string;
    json_object *element;
    int i;
    
    for (i = 0; i < len; i++) {
        field_key = fields[i];
        element = json_object_object_get(msg, field_key);
        if (element) {
            raw_string = json_object_get_string(element);
        } else {
            continue;
        }
        encrypted_string = md5_hash(raw_string);
        json_object_object_add(msg, field_key, json_object_new_string(encrypted_string));
        free(encrypted_string);
    }
}
