LIBEVENT ?= /usr/local
TARGET ?= /usr/local

CFLAGS = -I. -I$(LIBEVENT)/include -g
AR = ar
AR_FLAGS = rc
RANLIB = ranlib

libpubsubclient.a: pubsubclient.o stream_request.o
	/bin/rm -f $@
	$(AR) $(AR_FLAGS) $@ $^
	$(RANLIB) $@

all: libpubsubclient.a

install:
	/usr/bin/install -d $(TARGET)/lib/
	/usr/bin/install -d $(TARGET)/bin/
	/usr/bin/install -d $(TARGET)/include/pubsubclient
	/usr/bin/install libpubsubclient.a $(TARGET)/lib/
	/usr/bin/install pubsubclient.h $(TARGET)/include/pubsubclient

clean:
	/bin/rm -f *.a *.o
