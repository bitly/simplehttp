LIBEVENT ?= /usr/local
TARGET ?= /usr/local
LIBSIMPLEHTTP ?= ../simplehttp
LIBSIMPLEHTTP_INC ?= $(LIBSIMPLEHTTP)/..
LIBSIMPLEHTTP_LIB ?= $(LIBSIMPLEHTTP)

CFLAGS = -I$(LIBSIMPLEHTTP_INC) -I$(LIBEVENT)/include -Wall -g -O2
LIBS = -L$(LIBSIMPLEHTTP_LIB) -L$(LIBEVENT)/lib -levent -lsimplehttp

sortdb: sortdb.c
	$(CC) $(CFLAGS) -o $@ $< $(LIBS)

install:
	/usr/bin/install sortdb $(TARGET)/bin

clean:
	rm -rf *.a *.o sortdb *.dSYM test_output test.db
