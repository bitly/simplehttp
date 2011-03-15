#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include "stat.h"
#include "simplehttp.h"
#include "util.h"

static int64_t *stats = NULL;
static int *stats_idx = NULL;
static uint64_t *stats_counts = NULL;

extern int callback_count;
extern uint64_t request_count;

void simplehttp_stats_store(int index, unsigned int val)
{
    stats[(index * STAT_WINDOW) + stats_idx[index]] = val;
    stats_idx[index]++;
    stats_counts[index]++;
    
    if (stats_idx[index] >= STAT_WINDOW) {
        stats_idx[index] = 0;
    }
}

void simplehttp_stats_init()
{
    int i;
    stats = malloc(STAT_WINDOW * callback_count * sizeof(int64_t));
    for (i = 0; i < (STAT_WINDOW * callback_count); i++) {
        stats[i] = -1;
    }
    stats_idx = calloc(callback_count, sizeof(int));
    stats_counts = calloc(callback_count, sizeof(uint64_t));
}

void simplehttp_stats_destruct()
{
    free(stats);
    free(stats_idx);
    free(stats_counts);
}

struct simplehttp_stats *simplehttp_stats_new()
{
    struct simplehttp_stats *st;
    
    st = malloc(sizeof(struct simplehttp_stats));
    
    return st;
}

void simplehttp_stats_free(struct simplehttp_stats *st)
{
    int i;
    
    if (st) {
        if (st->stats_counts) {
            free(st->stats_counts);
        }
        
        if (st->average_requests) {
            free(st->average_requests);
        }
        
        if (st->ninety_five_percents) {
            free(st->ninety_five_percents);
        }
        
        if (st->stats_labels) {
            for (i = 0; i < st->callback_count; i++) {
                free(st->stats_labels[i]);
            }
            free(st->stats_labels);
        }
        
        free(st);
    }
}

void simplehttp_stats(struct simplehttp_stats *st)
{
    uint64_t request_total;
    int i, j, c, request_array_end;
    
    st->requests = request_count;
    st->callback_count = callback_count;
    st->stats_counts = malloc(callback_count * sizeof(uint64_t));
    memcpy(st->stats_counts, stats_counts, callback_count * sizeof(uint64_t));
    st->average_requests = calloc(callback_count, sizeof(uint64_t));
    st->ninety_five_percents = calloc(callback_count, sizeof(uint64_t));
    st->stats_labels = simplehttp_callback_names();
    
    for (i = 0; i < callback_count; i++) {
        request_total = 0;
        for (j = (i * STAT_WINDOW), request_array_end = j + STAT_WINDOW, c = 0; 
            (j < request_array_end) && (stats[j] != -1); j++, c++) {
            request_total += stats[j];
        }
        if (c) {
            st->average_requests[i] = request_total / c;
            st->ninety_five_percents[i] = ninety_five_percent(stats + (i * STAT_WINDOW), c);
        }
    }
}
