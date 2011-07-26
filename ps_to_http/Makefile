LIBEVENT ?= /usr/local
TARGET ?= /usr/local
LIBSIMPLEHTTP ?= ../simplehttp
LIBSIMPLEHTTP_INC ?= $(LIBSIMPLEHTTP)/..
LIBSIMPLEHTTP_LIB ?= $(LIBSIMPLEHTTP)
LIBPUBSUBCLIENT ?= ../pubsubclient

CFLAGS = -I. -I$(LIBSIMPLEHTTP_INC) -I$(LIBEVENT)/include -g -Wall -O2
LIBS = -L. -L$(LIBSIMPLEHTTP_LIB) -L$(LIBPUBSUBCLIENT) -L$(LIBEVENT)/lib -levent -lpubsubclient -lsimplehttp -lm

all: ps_to_http

ps_to_http: ps_to_http.c
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS)

install:
	/usr/bin/install -D ps_to_http $(TARGET)/bin/ps_to_http

clean:
	rm -rf *.o ps_to_http *.dSYM
