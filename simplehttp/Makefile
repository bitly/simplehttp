LIBEVENT ?= /usr/local
TARGET ?= /usr/local

CFLAGS = -I. -I$(LIBEVENT)/include -Wall -g -O2
LIBS = -L. -L$(LIBEVENT)/lib -levent -lm

AR = ar
AR_FLAGS = rc
RANLIB = ranlib

libsimplehttp.a: simplehttp.o async_simplehttp.o timer.o log.o util.o stat.o request.o
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
	/usr/bin/install queue.h $(TARGET)/include/simplehttp/

clean:
	rm -rf *.a *.o testserver *.dSYM
