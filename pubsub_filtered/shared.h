#ifndef PUBSUB_FILTERED_SHARED_H
#define PUBSUB_FILTERED_SHARED_H

#include <stdio.h>
#include <json/json.h>

int parse_fields(const char *str, char **field_array, int array_len, const char *type, FILE *log);
void free_fields(char **field_array, int array_len);
char *md5_hash(const char *string);
int filter_message_simple(const char *key, const char *value, struct json_object *msg);
void delete_fields(char **fields, int len, struct json_object *msg);
void encrypt_fields(char **fields, int len, struct json_object *msg);

#endif
