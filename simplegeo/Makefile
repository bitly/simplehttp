LIBEVENT ?= /usr/local
TARGET ?= /usr/local
LIBSIMPLEHTTP ?= ../simplehttp
LIBSIMPLEHTTP_INC ?= $(LIBSIMPLEHTTP)/..
LIBSIMPLEHTTP_LIB ?= $(LIBSIMPLEHTTP)

CFLAGS = -I. -I$(LIBSIMPLEHTTP_INC) -I$(LIBEVENT)/include -g
LIBS = -L. -L$(LIBSIMPLEHTTP_INC) -L$(LIBEVENT)/lib -L/usr/local/lib -levent -ljson -ltokyotyrant -ltokyocabinet -lz -lbz2 -lresolv -ldl -lpthread -lm -lc

simplegeo: simplegeo.c
	$(CC) $(CFLAGS) -o $@ $< $(LIBS) -lsimplehttp

install:
	/usr/bin/install -d $(TARGET)/bin
	/usr/bin/install simplegeo $(TARGET)/bin

clean:
	rm -rf *.o simplegeo *.dSYM
