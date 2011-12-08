TARGET ?= /usr/local
LIBEVENT ?= /usr/local
LIBSIMPLEHTTP ?= /usr/local

CFLAGS = -I. -I$(LIBSIMPLEHTTP)/include -I.. -I$(LIBEVENT)/include -g -Wall -O2
AR = ar
AR_FLAGS = rc
RANLIB = ranlib

libqueuereader.a: queuereader.o queuereader.h
	/bin/rm -f $@
	$(AR) $(AR_FLAGS) $@ $^
	$(RANLIB) $@

all: libqueuereader.a

install:
	/usr/bin/install -d $(TARGET)/lib/
	/usr/bin/install -d $(TARGET)/bin/
	/usr/bin/install -d $(TARGET)/include/queuereader
	/usr/bin/install libqueuereader.a $(TARGET)/lib/
	/usr/bin/install queuereader.h $(TARGET)/include/queuereader

clean:
	/bin/rm -f *.a *.o
