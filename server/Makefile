CFLAGS = -I. -I$(LIBEVENT)/include -Wall -g
LIBS = -L. -L$(LIBEVENT)/lib -levent
AR = ar
AR_FLAGS = rc
RANLIB = ranlib

libsimplehttp.a: simplehttp.o
	/bin/rm -f $@
	$(AR) $(AR_FLAGS) $@ $<
	$(RANLIB) $@

testserver: testserver.c
	$(CC) $(CFLAGS) -o $@ $< $(LIBS) -lsimplehttp

clean:
	rm -rf *.a *.o testserver *.dSYM
