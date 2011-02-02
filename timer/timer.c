#include <time.h>
#include <unistd.h>

#if _POSIX_TIMERS > 0

void _gettime(struct timespec *ts)
{
    clock_gettime(CLOCK_REALTIME, ts);
}

unsigned int _ts_diff(struct timespec start, struct timespec end)
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

void _gettime(struct timeval *ts)
{
    gettimeofday(ts, NULL);
}

unsigned int _ts_diff(struct timeval start, struct timeval end)
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
