LIBEVENT ?= /usr/local
TARGET ?= /usr/local
LIBSIMPLEHTTP ?= /usr/local
LIBPUBSUBCLIENT ?= /usr/local

CFLAGS = -I. -I$(LIBSIMPLEHTTP)/include -I$(LIBPUBSUBCLIENT)/include -I.. -I$(LIBEVENT)/include -g -Wall -O2
LIBS = -L. -L$(LIBSIMPLEHTTP)/lib -L$(LIBPUBSUBCLIENT)/lib -L../simplehttp -L../pubsubclient -L$(LIBEVENT)/lib -levent -lpubsubclient -lsimplehttp -lm

all: ps_to_http

ps_to_http: ps_to_http.c
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS)

install:
	/usr/bin/install -d $(TARGET)/bin/
	/usr/bin/install ps_to_http $(TARGET)/bin/

clean:
	rm -rf *.o ps_to_http *.dSYM
