#ifndef _STR_LIST_SET_H
#define _STR_LIST_SET_H

#include <event.h>
#include <simplehttp/uthash.h>

struct SetItem {
    const char *value;
    UT_hash_handle hh;
};

struct ListInfo {
    char sep;
    char sepstr[2];
    char *saveptr;
    char **buf;
    size_t buflen;
};

void prepare_token_list(struct ListInfo *list_info, char **db_data, size_t db_data_len, char sep);
char *reverse_tokenize(struct ListInfo *list_info);
void reserialize_list(struct evbuffer *output, struct json_object *array, char **db_data, size_t db_data_len, char sep);
void deserialize_alloc_set(struct SetItem **set, char **db_data, size_t db_data_len, char sep);
void serialize_free_set(struct evbuffer *output, struct json_object *array, struct SetItem **set, char sep);

static inline void serialize_list_item(struct evbuffer *output, const char *item, char sep)
{
    if (EVBUFFER_LENGTH(output) > 0) {
        evbuffer_add_printf(output, "%c", sep);
    }
    evbuffer_add_printf(output, "%s", item);
}

static inline void add_new_set_item(struct SetItem **set, const char *value_ptr)
{
    struct SetItem *set_item;
    set_item = calloc(1, sizeof(struct SetItem));
    set_item->value = value_ptr;
    HASH_ADD_KEYPTR(hh, *set, value_ptr, strlen(value_ptr), set_item);
}

#define TOKEN_LIST_FOREACH(item, list_info)      \
    for (item = strtok_r(*(list_info)->buf,      \
                          (list_info)->sepstr,   \
                         &(list_info)->saveptr); \
         item != NULL;                           \
         item = strtok_r(NULL,                   \
                          (list_info)->sepstr,   \
                         &(list_info)->saveptr) )\
 
#define TOKEN_LIST_FOREACH_REVERSE(item, list_info)      \
    while ((item = reverse_tokenize(list_info)) != NULL) \

#endif
