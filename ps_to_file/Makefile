LIBEVENT ?= /usr/local
TARGET ?= /usr/local
LIBSIMPLEHTTP ?= /usr/local
LIBPUBSUBCLIENT ?= /usr/local

CFLAGS = -I. -I$(LIBSIMPLEHTTP)/include -I$(LIBPUBSUBCLIENT)/include -I.. -I$(LIBEVENT)/include -g -Wall -O2
LIBS = -L. -L$(LIBSIMPLEHTTP)/lib -L$(LIBPUBSUBCLIENT)/lib -L../simplehttp -L../pubsubclient -L$(LIBEVENT)/lib -levent -lpubsubclient -lsimplehttp -lm

all: ps_to_file

ps_to_file: ps_to_file.c
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS)

install:
	/usr/bin/install -D ps_to_file $(TARGET)/bin/ps_to_file

clean:
	rm -f *.o *.a ps_to_file
