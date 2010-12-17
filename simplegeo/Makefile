LIBEVENT ?= /usr/local
LIBSIMPLEHTTP ?= ../simplehttp
LIBSIMPLEHTTP_INC ?= $(LIBSIMPLEHTTP)
LIBSIMPLEHTTP_LIB ?= $(LIBSIMPLEHTTP)

CFLAGS = -I. -I$(LIBSIMPLEHTTP_INC) -I$(LIBEVENT)/include -g
LIBS = -L. -L$(LIBSIMPLEHTTP_INC) -L$(LIBEVENT)/lib -L/usr/local/lib -levent -ljson -ltokyotyrant -ltokyocabinet -lz -lbz2 -lresolv -ldl -lpthread -lm -lc

simplegeo: simplegeo.c
	$(CC) $(CFLAGS) -o $@ $< $(LIBS) -lsimplehttp

clean:
	rm -rf *.o simplegeo *.dSYM
