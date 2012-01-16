TARGET ?= /usr/local
LIBEVENT ?= /usr/local
LIBSIMPLEHTTP ?= /usr/local

CFLAGS = -I. -I$(LIBSIMPLEHTTP)/include -I.. -I../simplehttp -I$(LIBEVENT)/include -g -Wall -O2
AR = ar
AR_FLAGS = rc
RANLIB = ranlib

libhost_pool.a: host_pool.o host_pool.h
	/bin/rm -f $@
	$(AR) $(AR_FLAGS) $@ $^
	$(RANLIB) $@

all: libhost_pool.a

install:
	/usr/bin/install -d $(TARGET)/lib/
	/usr/bin/install -d $(TARGET)/bin/
	/usr/bin/install -d $(TARGET)/include/host_pool
	/usr/bin/install libhost_pool.a $(TARGET)/lib/
	/usr/bin/install host_pool.h $(TARGET)/include/host_pool

clean:
	/bin/rm -f *.a *.o
