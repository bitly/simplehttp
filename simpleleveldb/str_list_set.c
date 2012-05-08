#include <stdlib.h>
#include <json/json.h>
#include "str_list_set.h"

void prepare_token_list(struct ListInfo *list_info, char **db_data, size_t db_data_len, char sep)
{
    // null terminate
    *db_data = realloc(*db_data, db_data_len + 1);
    *(*db_data + db_data_len) = '\0';
    
    list_info->sep = sep;
    list_info->buf = db_data;
    list_info->buflen = db_data_len;
    
    // for strtok_r()
    list_info->sepstr[0] = sep;
    list_info->sepstr[1] = '\0';
    
    // for reverse_tokenize()
    list_info->saveptr = *db_data + db_data_len;
}

// somewhat like strtok_r() but depends on prepare_token_list() for setup
char *reverse_tokenize(struct ListInfo *list_info)
{
    while (list_info->saveptr > *list_info->buf) {
        list_info->saveptr--;
        if (list_info->saveptr == *list_info->buf) {
            if (*list_info->saveptr != list_info->sep) {
                return list_info->saveptr;
            } else {
                return NULL;
            }
        }
        if (*list_info->saveptr == list_info->sep) {
            *list_info->saveptr = '\0';
            if (*(list_info->saveptr + 1) != '\0') {
                return list_info->saveptr + 1;
            }
        }
    }
    return NULL;
}

void reserialize_list(struct evbuffer *output, struct json_object *array, char **db_data, size_t db_data_len, char sep)
{
    struct ListInfo list_info;
    char *item;
    
    prepare_token_list(&list_info, db_data, db_data_len, sep);
    
    TOKEN_LIST_FOREACH(item, &list_info) {
        serialize_list_item(output, item, sep);
        
        if (array) {
            json_object_array_add(array, json_object_new_string(item));
        }
    }
}

void deserialize_alloc_set(struct SetItem **set, char **db_data, size_t db_data_len, char sep)
{
    char *token;
    struct SetItem *set_item;
    struct ListInfo list_info;
    
    prepare_token_list(&list_info, db_data, db_data_len, sep);
    
    TOKEN_LIST_FOREACH(token, &list_info) {
        HASH_FIND_STR(*set, token, set_item);
        if (!set_item) {
            add_new_set_item(set, token);
        }
    }
}

void serialize_free_set(struct evbuffer *output, struct json_object *array, struct SetItem **set, char sep)
{
    struct SetItem *set_item, *set_tmp;
    
    HASH_ITER(hh, *set, set_item, set_tmp) {
        if (array) {
            json_object_array_add(array, json_object_new_string(set_item->value));
        }
        serialize_list_item(output, set_item->value, sep);
        HASH_DEL(*set, set_item);
        free(set_item);
    }
}

