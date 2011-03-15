LIBEVENT ?= /usr/local
TARGET ?= /usr/local
LIBSIMPLEHTTP ?= ../simplehttp
LIBSIMPLEHTTP_INC ?= $(LIBSIMPLEHTTP)/..
LIBSIMPLEHTTP_LIB ?= $(LIBSIMPLEHTTP)

CFLAGS = -I. -I$(LIBSIMPLEHTTP_INC) -I$(LIBEVENT)/include -g
LIBS = -L. -L$(LIBSIMPLEHTTP_LIB) -L$(LIBEVENT)/lib -levent -ldl -lm -lc -lsimplehttp -lpubsubclient -lpthread -lcurl
AR = ar
AR_FLAGS = rc
RANLIB = ranlib

libpubsubclient.a: pubsubclient.o
	/bin/rm -f $@
	$(AR) $(AR_FLAGS) $@ $<
	$(RANLIB) $@

pubsub-reader: pubsub-reader.o
	$(CC) $(CFLAGS) -o $@ $< $(LIBS) 

all: libpubsubclient.a pubsub-reader

install:
	/usr/bin/install -d $(TARGET)/lib/
	/usr/bin/install -d $(TARGET)/bin/
	/usr/bin/install -d $(TARGET)/include/simplehttp
	/usr/bin/install pubsub-reader $(TARGET)/bin/
	/usr/bin/install pubsub2simplequeue.sh $(TARGET)/bin/
	/usr/bin/install libpubsubclient.a $(TARGET)/lib/
	/usr/bin/install pubsubclient.h $(TARGET)/include/simplehttp

clean:
	/bin/rm -f *.a *.o pubsub-reader
