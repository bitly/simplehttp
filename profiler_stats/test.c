#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <inttypes.h>
#include <simplehttp/utlist.h>
#include "profiler_stats.h"

int main(int argc, char **argv)
{
    int i;
    profiler_ts ts;
    struct ProfilerStat *pstat;
    struct ProfilerReturn *ret;
    
    profiler_stats_init(300000000);
    
    pstat = profiler_new_stat("test");
    
    for (i = 0; i < 20000; i++) {
        profiler_ts_get(&ts);
        profiler_stats_store_value(pstat, i, ts);
    }
    
    LL_FOREACH(profiler_stats_get_all(), pstat) {
        ret = profiler_get_stats(pstat);
        assert(ret->average == 17499);
        assert(ret->ninety_five_percent == 19751);
        assert(ret->ninety_nine_percent == 19951);
        assert(ret->hundred_percent == 19999);
        assert(ret->count == 20000);
        free(ret);
    }
    
    fprintf(stdout, "ok\n");
    
    return 0;
}
