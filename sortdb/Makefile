LIBEVENT ?= /usr/local
LIBSIMPLEHTTP ?= ../simplehttp
LIBSIMPLEHTTP_INC ?= $(LIBSIMPLEHTTP)
LIBSIMPLEHTTP_LIB ?= $(LIBSIMPLEHTTP)

CFLAGS = -I$(SIMPLEHTTP_INC) -I$(LIBEVENT)/include -Wall -g -O2
LIBS = -L$(SIMPLEHTTP_LIB) -L$(LIBEVENT)/lib -levent -lsimplehttp

sortdb: sortdb.c
	$(CC) $(CFLAGS) -o $@ $< $(LIBS)

install:
	/usr/bin/install sortdb $(LIBEVENT)/bin

clean:
	rm -rf *.a *.o sortdb *.dSYM
