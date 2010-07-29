# sortdb makefile
# 
# override relative include/lib dirs with env variables:
# export SIMPLEHTTP_INC="/dork/include"
# export SIMPLEHTTP_LIB="/dork/lib"

LIBEVENT ?= /usr/local
SIMPLEHTTP_INC ?= ../server
SIMPLEHTTP_LIB ?= ../server

CFLAGS = -I$(SIMPLEHTTP_INC) -I$(LIBEVENT)/include -Wall -g -O2
LIBS = -L$(SIMPLEHTTP_LIB) -L$(LIBEVENT)/lib -levent  -lsimplehttp

sortdb: sortdb.c
	$(CC) $(CFLAGS) -o $@ $< $(LIBS)

install:
	/usr/bin/install sortdb $(LIBEVENT)/bin

clean:
	rm -rf *.a *.o sortdb *.dSYM
