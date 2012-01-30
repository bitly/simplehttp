#ifndef __profiler_stats_h
#define __profiler_stats_h

#define PROFILER_STATS_VERSION "0.1"

#if _POSIX_TIMERS > 0

    typedef struct timespec profiler_ts;
    
    void profiler_ts_get(struct timespec *ts);
    unsigned int profiler_ts_diff(struct timespec start, struct timespec end);

#else

    typedef struct timeval profiler_ts;
    
    void profiler_ts_get(struct timeval *ts);
    unsigned int profiler_ts_diff(struct timeval start, struct timeval end);

#endif

struct UT_hash_handle;

struct ProfilerReturn {
    uint64_t count;
    uint64_t average;
    uint64_t hundred_percent;
    uint64_t ninety_nine_percent;
    uint64_t ninety_five_percent;
};

struct ProfilerData {
    uint64_t value;
    profiler_ts ts;
    struct ProfilerData *prev;
    struct ProfilerData *next;
};

struct ProfilerStat {
    char *name;
    struct ProfilerData *data;
    uint64_t count;
    int index;
    UT_hash_handle hh;
    struct ProfilerStat *next;
};

void profiler_stats_reset();
void profiler_stats_store(const char *name, profiler_ts start_ts);
struct ProfilerReturn *profiler_get_stats_for_name(const char *name);
struct ProfilerReturn *profiler_get_stats(struct ProfilerStat *pstat);
struct ProfilerStat *profiler_stats_get_all();
void free_profiler_stats();

#endif
