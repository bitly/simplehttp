LIBEVENT ?= /usr/local
TARGET ?= /usr/local
LIBSIMPLEHTTP ?= ../simplehttp
LIBSIMPLEHTTP_INC ?= $(LIBSIMPLEHTTP)
LIBSIMPLEHTTP_LIB ?= $(LIBSIMPLEHTTP)

CFLAGS = -I. -I$(LIBSIMPLEHTTP_INC) -I$(LIBEVENT)/include -g -O2
LIBS = -L. -L$(LIBSIMPLEHTTP_LIB) -L$(LIBEVENT)/lib -levent -ljson -ldl -lm -lc
AR = ar
AR_FLAGS = rc
RANLIB = ranlib

libpubsub_to_pubsub.a: pubsub_to_pubsub.o
	/bin/rm -f $@
	$(AR) $(AR_FLAGS) $@ $<
	$(RANLIB) $@

ps_to_ps: ps_to_ps.c
	$(CC) $(CFLAGS) -o $@ $< $(LIBS) -lpubsub_to_pubsub

all: libpubsub_to_pubsub.a ps_to_ps

install:
	/usr/bin/install libpubsub_to_pubsub.a $(TARGET)/lib/
	/usr/bin/install -D pubsub_to_pubsub.h $(TARGET)/include/simplehttp/pubsub_to_pubsub.h
	/usr/bin/install ps_to_ps $(TARGET)/bin/

clean:
	/bin/rm -f libpubsub_to_pubsub.a pubsub_to_pubsub.o
	/bin/rm -f ps_to_ps
