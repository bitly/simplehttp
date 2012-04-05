#ifndef __profiler_stats_h
#define __profiler_stats_h

#include <inttypes.h>
#include <time.h>
#include <sys/time.h>

#define PROFILER_STATS_VERSION "0.3"

#if _POSIX_TIMERS > 0

typedef struct timespec profiler_ts;

inline void profiler_ts_get(struct timespec *ts);
inline unsigned int profiler_ts_diff(struct timespec start, struct timespec end);

#else

typedef struct timeval profiler_ts;

inline void profiler_ts_get(struct timeval *ts);
inline unsigned int profiler_ts_diff(struct timeval start, struct timeval end);

#endif

struct ProfilerReturn {
    uint64_t count;
    uint64_t average;
    uint64_t hundred_percent;
    uint64_t ninety_nine_percent;
    uint64_t ninety_five_percent;
};

struct ProfilerData {
    int64_t value;
    profiler_ts ts;
};

struct ProfilerStat {
    char *name;
    struct ProfilerData **data;
    uint64_t count;
    int index;
    struct ProfilerStat *next;
};

void profiler_stats_init(int window_usec);
struct ProfilerStat *profiler_new_stat(const char *name);
void free_profiler_stats();
void profiler_stats_reset();
inline void profiler_stats_store(const char *name, profiler_ts start_ts);
inline void profiler_stats_store_for_name(const char *name, uint64_t val, profiler_ts ts);
inline void profiler_stats_store_value(struct ProfilerStat *pstat, uint64_t val, profiler_ts ts);
struct ProfilerReturn *profiler_get_stats_for_name(const char *name);
struct ProfilerReturn *profiler_get_stats(struct ProfilerStat *pstat);
struct ProfilerStat *profiler_stats_get_all();

#endif
