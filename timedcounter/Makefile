CFLAGS = -I. -I../server -I$(LIBEVENT)/include -g -O2
LIBS = -L. -L../server -L$(LIBEVENT)/lib -levent -ltcl

timedcounter: timedcounter.c
	$(CC) $(CFLAGS) -o $@ $< $(LIBS) -lsimplehttp

clean:
	rm -rf *.o timedcounter *.dSYM
