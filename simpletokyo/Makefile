LIBEVENT ?= /usr/local
LIBSIMPLEHTTP ?= ../simplehttp
LIBSIMPLEHTTP_INC ?= $(LIBSIMPLEHTTP)
LIBSIMPLEHTTP_LIB ?= $(LIBSIMPLEHTTP)

CFLAGS = -I. -I$(LIBSIMPLEHTTP_INC) -I$(LIBEVENT)/include -Wall -g -O2
LIBS = -L. -L$(LIBSIMPLEHTTP_LIB) -L$(LIBEVENT)/lib -L/usr/local/lib -levent -ljson -ltokyotyrant -ltokyocabinet -lsimplehttp

simpletokyo: simpletokyo.c
	$(CC) $(CFLAGS) -o $@ $< $(LIBS)

install:
	/usr/bin/install -D simpletokyo $(LIBEVENT)/bin/simpletokyo

clean:
	rm -rf *.o simpletokyo *.dSYM
