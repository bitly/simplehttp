LIBEVENT ?= /usr/local
TARGET ?= /usr/local
LIBSIMPLEHTTP ?= /usr/local

CFLAGS = -I. -I$(LIBSIMPLEHTTP)/include -I.. -I$(LIBEVENT)/include -g 
LIBS = -L. -L$(LIBSIMPLEHTTP)/lib -L../simplehttp -L$(LIBEVENT)/lib -levent -lsimplehttp -ljson -lpcre -lm -lpubsubclient -lcrypto

pubsub_filtered: pubsub_filtered.c md5.c
	$(CC) $(CFLAGS) -o $@ md5.c $< $(LIBS)

install:
	/usr/bin/install -d $(TARGET)/bin/
	/usr/bin/install pubsub_filtered $(TARGET)/bin/

clean:
	rm -rf *.o pubsub_filtered *.dSYM
