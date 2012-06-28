LIBEVENT ?= /usr/local
TARGET ?= /usr/local
LIBSIMPLEHTTP ?= /usr/local

CFLAGS = -I. -I$(LIBSIMPLEHTTP)/include -I.. -I$(LIBEVENT)/include -g 
LIBS = -L. -L$(LIBSIMPLEHTTP)/lib -L../simplehttp -L$(LIBEVENT)/lib -levent -lsimplehttp -ljson -lpcre -lm -lpubsubclient -lcrypto

LIBS_STREAM_FILTER = -L. -L$(LIBSIMPLEHTTP)/lib -L../simplehttp -lsimplehttp -ljson

pubsub_filtered: pubsub_filtered.c md5.c
	$(CC) $(CFLAGS) -o $@ md5.c $< $(LIBS)

stream_filter: stream_filter.c md5.c
	$(CC) $(CFLAGS) -o $@ md5.c $< $(LIBS_STREAM_FILTER)

install:
	/usr/bin/install -d $(TARGET)/bin/
	/usr/bin/install pubsub_filtered $(TARGET)/bin/

install_stream_filter:
	/usr/bin/install -d $(TARGET)/bin/
	/usr/bin/install stream_filter $(TARGET)/bin/

clean:
	rm -rf *.o pubsub_filtered stream_filter *.dSYM
