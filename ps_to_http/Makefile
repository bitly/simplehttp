LIBEVENT ?= /usr/local
TARGET ?= /usr/local
LIBSIMPLEHTTP ?= /usr/local
LIBPUBSUBCLIENT ?= /usr/local

CFLAGS = -I. -I$(LIBSIMPLEHTTP)/include -I$(LIBPUBSUBCLIENT)/include -I$(LIBEVENT)/include -I.. -g -Wall -O2
LIBS = -L. -L$(LIBSIMPLEHTTP)/lib -L$(LIBPUBSUBCLIENT)/lib -L$(LIBEVENT)/lib -L../pubsubclient -L../simplehttp -levent -lpubsubclient -lsimplehttp -lm

all: ps_to_http

ps_to_http: ps_to_http.c
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS)

install:
	/usr/bin/install -D ps_to_http $(TARGET)/bin/ps_to_http

clean:
	rm -rf *.o ps_to_http *.dSYM
