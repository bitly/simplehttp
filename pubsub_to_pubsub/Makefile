CFLAGS = -I. -I$(LIBEVENT)/include -g -O2
LIBS = -L. -I$(LIBEVENT)/lib -levent -ljson -ldl -lm -lc
AR = ar
AR_FLAGS = rc
RANLIB = ranlib

libpubsub_to_pubsub.a: pubsub_to_pubsub.o
	/bin/rm -f $@
	$(AR) $(AR_FLAGS) $@ $<
	$(RANLIB) $@

all: libpubsub_to_pubsub.a

install:
	/usr/bin/install libpubsub_to_pubsub.a $(LIBEVENT)/lib/
	/usr/bin/install -D pubsub_to_pubsub.h $(LIBEVENT)/include/simplehttp/pubsub_to_pubsub.h

clean:
	/bin/rm -f libpubsub_to_pubsub.a pubsub_to_pubsub.o
