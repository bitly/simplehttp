LIBEVENT ?= /usr/local
LIBSIMPLEHTTP ?= ../simplehttp
LIBSIMPLEHTTP_INC ?= $(LIBSIMPLEHTTP)
LIBSIMPLEHTTP_LIB ?= $(LIBSIMPLEHTTP)

CFLAGS = -I$(LIBSIMPLEHTTP_INC) -I$(LIBEVENT)/include -Wall -g -O2
LIBS = -L$(LIBSIMPLEHTTP_LIB) -L$(LIBEVENT)/lib -levent -lsimplehttp

simplequeue: simplequeue.c
	$(CC) $(CFLAGS) -o $@ $< $(LIBS)

install:
	/usr/bin/install simplequeue $(LIBEVENT)/bin

clean:
	rm -rf *.a *.o simplequeue *.dSYM
