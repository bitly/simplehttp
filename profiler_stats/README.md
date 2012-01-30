profiler_stats
=============

a library to track arbitrary profiler timings for average, 95%, 99%, 100% time

the library enforces a rolling window for data collection.  currently it will consider either 5000 entries or the 
last 5 minutes of entries, whichever is smaller.

compiling
---------

there are no additional dependencies other than client applications needing to link against `libm` aka `-lm`

```
cd simplehttp/profiler_stats
make
make install
```

usage
-----

no initialization is required, the general process is as follows (NOTE: client is responsible 
for `free()` of all returned structures as well as a final call to `profiler_stats_free()`):

```
#include <profiler_stats/profiler_stats.h>
#include <utlist.h>

void my_function()
{
    profiler_ts start_ts;
    
    profiler_get_ts(&start_ts);
    
    // do some work
    
    profiler_stats_store("stat_name", start_ts);
}

void my_stats_output()
{
    struct ProfilerStat *pstat;
    struct ProfilerReturn *ret;
    
    // LL_FOREACH() is from utlist.h http://uthash.sourceforge.net/
    LL_FOREACH(profiler_stats_get_all(), pstat) {
        ret = profiler_get_stats(pstat);
        fprintf(stdout, "100%%: %"PRIu64"\n", ret->hundred_percent);
        fprintf(stdout, "99%%: %"PRIu64"\n", ret->ninety_nine_percent);
        fprintf(stdout, "95%%: %"PRIu64"\n", ret->ninety_five_percent);
        fprintf(stdout, "avg: %"PRIu64"\n", ret->average);
        fprintf(stdout, "count: %"PRIu64"\n", ret->count);
        free(ret);
    }
}
```