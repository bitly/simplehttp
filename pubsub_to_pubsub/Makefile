CFLAGS = -I. -I$(LIBEVENT)/include -g -O2
LIBS = -L. -I$(LIBEVENT)/lib -levent -ljson -ldl -lm -lc
AR = ar
AR_FLAGS = rc
RANLIB = ranlib

libpubsub_to_pubsub.a: pubsub_to_pubsub.o
	/bin/rm -f $@
	$(AR) $(AR_FLAGS) $@ $<
	$(RANLIB) $@

clean:
	/bin/rm -f libpubsub_to_pubsub.a pubsub_to_pubsub.o
