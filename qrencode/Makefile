LIBEVENT ?= /usr/local
LIBSIMPLEHTTP ?= ../simplehttp
LIBSIMPLEHTTP_INC ?= $(LIBSIMPLEHTTP)
LIBSIMPLEHTTP_LIB ?= $(LIBSIMPLEHTTP)

CFLAGS = -I. -I$(LIBSIMPLEHTTP_INC) -I$(LIBEVENT)/include -g
LIBS = -L. -L$(LIBSIMPLEHTTP_LIB) -L$(LIBEVENT)/lib -L/usr/local/lib -levent -lqrencode -lpng -lz -lbz2 -lresolv -ldl -lpthread -lm -lc

qrencode: qrencode.c
	$(CC) $(CFLAGS) -o $@ $< $(LIBS) -lsimplehttp

clean:
	rm -rf *.o qrencode *.dSYM
