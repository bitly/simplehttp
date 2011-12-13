LIBEVENT ?= /usr/local
TARGET ?= /usr/local
LIBSIMPLEHTTP ?= ../simplehttp
LIBSIMPLEHTTP_INC ?= $(LIBSIMPLEHTTP)/..
LIBSIMPLEHTTP_LIB ?= $(LIBSIMPLEHTTP)
LIBLEVELDB ?= /usr/local

CFLAGS = -I. -I$(LIBSIMPLEHTTP_INC) -I$(LIBEVENT)/include -I$(LIBLEVELDB)/include -Wall -g -O2
LIBS = -L. -L$(LIBSIMPLEHTTP_LIB) -L$(LIBEVENT)/lib -L/usr/local/lib -L$(LIBLEVELDB)/lib -levent -ljson -lsimplehttp -lleveldb -lm -lstdc++
AR = ar
AR_FLAGS = rc
RANLIB = ranlib

all: simpleleveldb

simpleleveldb: simpleleveldb.c
	$(CC) $(CFLAGS) -o $@ simpleleveldb.c $(LIBS)

install:
	/usr/bin/install -d $(TARGET)/bin/
	/usr/bin/install simpleleveldb $(TARGET)/bin/

clean:
	rm -rf *.a *.o simpleleveldb *.dSYM
