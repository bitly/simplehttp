#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <inttypes.h>
#include <assert.h>
#include <time.h>
#include <sys/time.h>
#include <math.h>
#include <simplehttp/uthash.h>
#include <simplehttp/utlist.h>
#include "profiler_stats.h"

#ifdef DEBUG
#define _DEBUG(...) fprintf(stdout, __VA_ARGS__)
#else
#define _DEBUG(...) do {;} while (0)
#endif

#define STAT_WINDOW_COUNT 5000

struct ProfilerEntry {
    struct ProfilerStat *stat;
    UT_hash_handle hh;
};

static int stat_window_usec = 300000000;
static struct ProfilerEntry *profiler_entries = NULL;

void profiler_stats_init(int window_usec)
{
    stat_window_usec = window_usec;
}

struct ProfilerStat *profiler_new_stat(const char *name)
{
    struct ProfilerStat *pstat;
    struct ProfilerEntry *entry;
    static struct ProfilerStat *last = NULL;
    int i;
    
    pstat = malloc(sizeof(struct ProfilerStat));
    pstat->name = strdup(name);
    pstat->data = malloc(STAT_WINDOW_COUNT * sizeof(struct ProfilerData *));
    for (i = 0; i < STAT_WINDOW_COUNT; i++) {
        pstat->data[i] = malloc(sizeof(struct ProfilerData));
        pstat->data[i]->value = -1;
    }
    pstat->count = 0;
    pstat->index = 0;
    pstat->next = NULL;
    
    _DEBUG("%s: new ProfilerStat %p\n", __FUNCTION__, pstat);
    
    // build the linked list as we add stats
    if (last) {
        last->next = pstat;
    }
    last = pstat;
    
    entry = malloc(sizeof(struct ProfilerEntry));
    entry->stat = pstat;
    
    _DEBUG("%s: new ProfilerEntry %p\n", __FUNCTION__, entry);
    
    HASH_ADD_KEYPTR(hh, profiler_entries, pstat->name, strlen(pstat->name), entry);
    
    return pstat;
}

void free_profiler_stats()
{
    int i;
    struct ProfilerEntry *entry, *tmp_entry;
    
    HASH_ITER(hh, profiler_entries, entry, tmp_entry) {
        HASH_DELETE(hh, profiler_entries, entry);
        for (i = 0; i < STAT_WINDOW_COUNT; i++) {
            free(entry->stat->data[i]);
        }
        free(entry->stat->data);
        free(entry->stat->name);
        free(entry->stat);
        free(entry);
    }
}

void profiler_stats_reset()
{
    int i;
    struct ProfilerEntry *entry, *tmp_entry;
    struct ProfilerData *data;
    
    HASH_ITER(hh, profiler_entries, entry, tmp_entry) {
        entry->stat->count = 0;
        entry->stat->index = 0;
        for (i = 0; i < STAT_WINDOW_COUNT; i++) {
            data = entry->stat->data[i];
            data->value = -1;
        }
    }
}

inline void profiler_stats_store(const char *name, profiler_ts start_ts)
{
    profiler_ts end_ts;
    uint64_t diff;
    
    profiler_ts_get(&end_ts);
    diff = profiler_ts_diff(start_ts, end_ts);
    
    profiler_stats_store_for_name(name, diff, end_ts);
}

inline void profiler_stats_store_for_name(const char *name, uint64_t val, profiler_ts ts)
{
    struct ProfilerEntry *entry;
    struct ProfilerStat *pstat;
    
    HASH_FIND_STR(profiler_entries, name, entry);
    if (!entry) {
        pstat = profiler_new_stat(name);
    } else {
        pstat = entry->stat;
    }
    
    profiler_stats_store_value(pstat, val, ts);
}

inline void profiler_stats_store_value(struct ProfilerStat *pstat, uint64_t val, profiler_ts ts)
{
    struct ProfilerData *data;
    
    data = pstat->data[pstat->index];
    data->value = val;
    data->ts = ts;
    
    pstat->count++;
    pstat->index++;
    
    if (pstat->index >= STAT_WINDOW_COUNT) {
        pstat->index = 0;
    }
}

static int int_cmp(const void *a, const void *b)
{
    const uint64_t *ia = (const uint64_t *)a;
    const uint64_t *ib = (const uint64_t *)b;
    
    return *ia  - *ib;
}

static uint64_t percentile(float perc, uint64_t *sorted_array, int length)
{
    uint64_t value;
    int index_of_perc;
    
    index_of_perc = (int)ceil(((perc / 100.0) * length) + 0.5);
    if (index_of_perc >= length) {
        index_of_perc = length - 1;
    }
    value = sorted_array[index_of_perc];
    
    return value;
}

struct ProfilerReturn *profiler_get_stats_for_name(const char *name)
{
    struct ProfilerEntry *entry;
    HASH_FIND_STR(profiler_entries, name, entry);
    return entry ? profiler_get_stats(entry->stat) : NULL;
}

struct ProfilerReturn *profiler_get_stats(struct ProfilerStat *pstat)
{
    struct ProfilerData *data;
    struct ProfilerReturn *ret;
    uint64_t request_total;
    profiler_ts cur_ts;
    uint64_t diff;
    uint64_t int_array[STAT_WINDOW_COUNT];
    int valid_count;
    int c;
    int start_index;
    
    if (!pstat) {
        return NULL;
    }
    
    profiler_ts_get(&cur_ts);
    
    if (pstat->count <= STAT_WINDOW_COUNT) {
        start_index = 0;
    } else {
        start_index = pstat->index;
    }
    
    _DEBUG("%s: start_index = %d\n", __FUNCTION__, start_index);
    
    valid_count = 0;
    request_total = 0;
    for (c = 0; c < STAT_WINDOW_COUNT; c++) {
        data = pstat->data[(start_index + c) % STAT_WINDOW_COUNT];
        if (data->value != -1) {
            diff = profiler_ts_diff(data->ts, cur_ts);
            if (diff < stat_window_usec) {
                int_array[valid_count++] = data->value;
                request_total += data->value;
            }
        }
    }
    
    _DEBUG("%s: valid_count = %d\n", __FUNCTION__, valid_count);
    
    ret = malloc(sizeof(struct ProfilerReturn));
    ret->count = pstat->count;
    if (valid_count) {
        ret->average = request_total / valid_count;
        qsort(int_array, valid_count, sizeof(uint64_t), int_cmp);
        ret->hundred_percent = percentile(100.0, int_array, valid_count);
        ret->ninety_nine_percent = percentile(99.0, int_array, valid_count);
        ret->ninety_five_percent = percentile(95.0, int_array, valid_count);
    } else {
        ret->average = 0;
        ret->hundred_percent = 0;
        ret->ninety_nine_percent = 0;
        ret->ninety_five_percent = 0;
    }
    
    return ret;
}

struct ProfilerStat *profiler_stats_get_all()
{
    return profiler_entries->stat;
}

#if _POSIX_TIMERS > 0

inline void profiler_ts_get(struct timespec *ts)
{
    clock_gettime(CLOCK_REALTIME, ts);
}

inline unsigned int profiler_ts_diff(struct timespec start, struct timespec end)
{
    struct timespec temp;
    
    if ((end.tv_nsec - start.tv_nsec) < 0) {
        temp.tv_sec = end.tv_sec - start.tv_sec - 1;
        temp.tv_nsec = 1000000000 + end.tv_nsec - start.tv_nsec;
    } else {
        temp.tv_sec = end.tv_sec - start.tv_sec;
        temp.tv_nsec = end.tv_nsec - start.tv_nsec;
    }
    
    // return usec as int
    return (temp.tv_sec * 1000000) + (temp.tv_nsec / 1000);
}

#else

inline void profiler_ts_get(struct timeval *ts)
{
    gettimeofday(ts, NULL);
}

inline unsigned int profiler_ts_diff(struct timeval start, struct timeval end)
{
    struct timeval temp;

    if ((end.tv_usec - start.tv_usec) < 0) {
        temp.tv_sec = end.tv_sec - start.tv_sec - 1;
        temp.tv_usec = 1000000 + end.tv_usec - start.tv_usec;
    } else {
        temp.tv_sec = end.tv_sec - start.tv_sec;
        temp.tv_usec = end.tv_usec - start.tv_usec;
    }

    // return usec as int
    return (temp.tv_sec * 1000000) + temp.tv_usec;
}

#endif
