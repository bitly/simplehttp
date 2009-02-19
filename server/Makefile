CFLAGS = -I. -I$(LIBEVENT)/include -Wall -g
LIBS = -L$(LIBEVENT)/lib -levent

simplehttp: simplehttp.c
	$(CC) $(CFLAGS) -o $@ simplehttp.c $(LIBS)
clean:
	rm -f simplehttp *~