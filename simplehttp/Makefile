LIBEVENT ?= /usr/local
TARGET ?= /usr/local

CFLAGS = -I. -I$(LIBEVENT)/include -Wall -g
LIBS = -L. -L$(LIBEVENT)/lib -levent -lm

AR = ar
AR_FLAGS = rc
RANLIB = ranlib

libsimplehttp.a: simplehttp.o async_simplehttp.o timer.o log.o util.o stat.o request.o options.o
	/bin/rm -f $@
	$(AR) $(AR_FLAGS) $@ $^
	$(RANLIB) $@

testserver: testserver.c
	$(CC) $(CFLAGS) -o $@ $< $(LIBS) -lsimplehttp

all: libsimplehttp.a testserver

install:
	/usr/bin/install -d $(TARGET)/lib/
	/usr/bin/install -d $(TARGET)/include/simplehttp/
	/usr/bin/install libsimplehttp.a $(TARGET)/lib/
	/usr/bin/install simplehttp.h $(TARGET)/include/simplehttp/
	/usr/bin/install simplehttp.h $(TARGET)/include/simplehttp/
	/usr/bin/install queue.h $(TARGET)/include/simplehttp/
	/usr/bin/install uthash.h $(TARGET)/include/simplehttp/
	/usr/bin/install utlist.h $(TARGET)/include/simplehttp/
	/usr/bin/install utstring.h $(TARGET)/include/simplehttp/
	/usr/bin/install utarray.h $(TARGET)/include/simplehttp/
	/usr/bin/install options.h $(TARGET)/include/simplehttp/

clean:
	rm -rf *.a *.o testserver *.dSYM
