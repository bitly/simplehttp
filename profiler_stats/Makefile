TARGET ?= /usr/local

CFLAGS = -I. -I.. -g -Wall -O2
AR = ar
AR_FLAGS = rc
RANLIB = ranlib

LIBS = -L. -lm -lprofiler_stats

all: libprofiler_stats.a test
    
libprofiler_stats.a: profiler_stats.o profiler_stats.h
	/bin/rm -f $@
	$(AR) $(AR_FLAGS) $@ $^
	$(RANLIB) $@

test: test.c
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS)

install:
	/usr/bin/install -d $(TARGET)/lib/
	/usr/bin/install -d $(TARGET)/bin/
	/usr/bin/install -d $(TARGET)/include/profiler_stats
	/usr/bin/install libprofiler_stats.a $(TARGET)/lib/
	/usr/bin/install profiler_stats.h $(TARGET)/include/profiler_stats

clean:
	/bin/rm -f *.a *.o test
