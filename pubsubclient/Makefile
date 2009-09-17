CFLAGS = -I. -I$(LIBEVENT)/include -g -O2
LIBS = -L. -L$(LIBEVENT)/lib -levent -ldl -lm -lc 
AR = ar
AR_FLAGS = rc
RANLIB = ranlib

libpubsubclient.a: pubsubclient.o
	/bin/rm -f $@
	$(AR) $(AR_FLAGS) $@ $<
	$(RANLIB) $@

clean:
	/bin/rm -f libpubsubclient.a pubsubclient.o
