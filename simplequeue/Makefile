LIBEVENT ?= /usr/local
TARGET ?= /usr/local
LIBSIMPLEHTTP ?= ../simplehttp
LIBSIMPLEHTTP_INC ?= $(LIBSIMPLEHTTP)/..
LIBSIMPLEHTTP_LIB ?= $(LIBSIMPLEHTTP)

CFLAGS = -I$(LIBSIMPLEHTTP_INC) -I$(LIBEVENT)/include -Wall -g -O2
LIBS = -L$(LIBSIMPLEHTTP_LIB) -L$(LIBEVENT)/lib -levent -lsimplehttp -lm

simplequeue: simplequeue.c
	$(CC) $(CFLAGS) -o $@ $< $(LIBS)

install:
	/usr/bin/install -d $(TARGET)/bin
	/usr/bin/install simplequeue $(TARGET)/bin

clean:
	rm -rf *.a *.o simplequeue *.dSYM
