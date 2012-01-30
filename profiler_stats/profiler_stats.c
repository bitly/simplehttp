#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <assert.h>
#include <time.h>
#include <sys/time.h>
#include <math.h>
#include <simplehttp/uthash.h>
#include <simplehttp/utlist.h>
#include "profiler_stats.h"

#define STAT_WINDOW_USEC 300000000
#define STAT_WINDOW_COUNT 5000

static struct ProfilerStat *profiler_stats = NULL;

void profiler_stats_reset()
{
    struct ProfilerStat *pstat, *tmp_pstat;
    struct ProfilerData *data, *tmp_data;
    
    HASH_ITER(hh, profiler_stats, pstat, tmp_pstat) {
        pstat->count = 0;
        pstat->index = 0;
        DL_FOREACH_SAFE(pstat->data, data, tmp_data) {
            DL_DELETE(pstat->data, data);
            free(data);
        }
    }
}

void profiler_stats_store(const char *name, profiler_ts start_ts)
{
    struct ProfilerData *data;
    struct ProfilerStat *pstat;
    profiler_ts end_ts;
    uint64_t diff;
    
    profiler_ts_get(&end_ts);
    diff = profiler_ts_diff(start_ts, end_ts);
    
    HASH_FIND_STR(profiler_stats, name, pstat);
    if (!pstat) {
        pstat = calloc(1, sizeof(struct ProfilerStat));
        pstat->name = strdup(name);
        HASH_ADD_KEYPTR(hh, profiler_stats, name, strlen(pstat->name), pstat);
    }
    
    data = malloc(sizeof(struct ProfilerData));
    data->value = diff;
    data->ts = end_ts;
    
    pstat->count++;
    pstat->index++;
    DL_APPEND(pstat->data, data);
    
    if (pstat->index > STAT_WINDOW_COUNT) {
        // pop the oldest entry off the front
        DL_DELETE(pstat->data, pstat->data);
        pstat->index--;
    }
}

void free_profiler_stats()
{
    struct ProfilerStat *pstat, *tmp_pstat;
    struct ProfilerData *data, *tmp_data;
    
    HASH_ITER(hh, profiler_stats, pstat, tmp_pstat) {
        HASH_DELETE(hh, profiler_stats, pstat);
        DL_FOREACH_SAFE(pstat->data, data, tmp_data) {
            DL_DELETE(pstat->data, data);
            free(data);
        }
        free(pstat->name);
        free(pstat);
    }
}

static int int_cmp(const void *a, const void *b)
{
    const uint64_t *ia = (const uint64_t *)a;
    const uint64_t *ib = (const uint64_t *)b;
    
    return *ia  - *ib;
}

static uint64_t percentile(float perc, uint64_t *int_array, int length)
{
    uint64_t value;
    uint64_t *sorted_requests;
    int index_of_perc;
    
    sorted_requests = calloc(length, sizeof(uint64_t));
    memcpy(sorted_requests, int_array, length * sizeof(uint64_t));
    qsort(sorted_requests, length, sizeof(uint64_t), int_cmp);
    index_of_perc = (int)ceil(((perc / 100.0) * length) + 0.5);
    if (index_of_perc >= length) {
        index_of_perc = length - 1;
    }
    value = sorted_requests[index_of_perc];
    free(sorted_requests);
    
    return value;
}

struct ProfilerReturn *profiler_get_stats_for_name(const char *name)
{
    struct ProfilerStat *pstat;
    HASH_FIND_STR(profiler_stats, name, pstat);
    return profiler_get_stats(pstat);
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
    
    if (!pstat) {
        return NULL;
    }
    
    profiler_ts_get(&cur_ts);
    
    valid_count = 0;
    request_total = 0;
    DL_FOREACH(pstat->data, data) {
        diff = profiler_ts_diff(data->ts, cur_ts);
        if (diff < STAT_WINDOW_USEC) {
            int_array[valid_count++] = data->value;
            request_total += data->value;
        }
    }
    
    ret = malloc(sizeof(struct ProfilerReturn));
    ret->count = pstat->count;
    if (valid_count) {
        ret->average = request_total / valid_count;
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
    return profiler_stats;
}

#if _POSIX_TIMERS > 0

    void profiler_ts_get(struct timespec *ts)
    {
        clock_gettime(CLOCK_REALTIME, ts);
    }
    
    unsigned int profiler_ts_diff(struct timespec start, struct timespec end)
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

    void profiler_ts_get(struct timeval *ts)
    {
        gettimeofday(ts, NULL);
    }
    
    unsigned int profiler_ts_diff(struct timeval start, struct timeval end)
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
