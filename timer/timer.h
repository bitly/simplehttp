#if _POSIX_TIMERS > 0

static struct timespec ts1, ts2;

void _gettime(struct timespec *ts);
unsigned int _ts_diff(struct timespec start, struct timespec end);

#else

static struct timeval ts1, ts2;

void _gettime(struct timeval *ts);
unsigned int _ts_diff(struct timeval start, struct timeval end);

#endif
