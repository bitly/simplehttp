LIBEVENT ?= /usr/local
TARGET ?= /usr/local

CFLAGS = -I. -I$(LIBEVENT)/include -Wall -g -O2
LIBS = -L. -L$(LIBEVENT)/lib -levent

AR = ar
AR_FLAGS = rc
RANLIB = ranlib

libsimplehttp.a: simplehttp.o
	/bin/rm -f $@
	$(AR) $(AR_FLAGS) $@ $<
	$(RANLIB) $@

testserver: testserver.c
	$(CC) $(CFLAGS) -o $@ $< $(LIBS) -lsimplehttp

all: libsimplehttp.a testserver

install:
	/usr/bin/install libsimplehttp.a $(TARGET)/lib/
	/usr/bin/install -D simplehttp.h $(TARGET)/include/simplehttp/simplehttp.h
	/usr/bin/install -D queue.h $(TARGET)/include/simplehttp/queue.h

clean:
	rm -rf *.a *.o testserver *.dSYM
