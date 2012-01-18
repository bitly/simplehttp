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

all: simpleleveldb leveldb_to_csv csv_to_leveldb

leveldb_to_csv: leveldb_to_csv.c
	$(CC) $(CFLAGS) -o $@ leveldb_to_csv.c $(LIBS)

csv_to_leveldb: csv_to_leveldb.c
	$(CC) $(CFLAGS) -o $@ csv_to_leveldb.c $(LIBS)

simpleleveldb: simpleleveldb.c
	$(CC) $(CFLAGS) -o $@ simpleleveldb.c $(LIBS)

install:
	/usr/bin/install -d $(TARGET)/bin/
	/usr/bin/install simpleleveldb $(TARGET)/bin/
	/usr/bin/install leveldb_to_csv $(TARGET)/bin/
	/usr/bin/install csv_to_leveldb $(TARGET)/bin/

clean:
	rm -rf *.a *.o simpleleveldb leveldb_to_csv csv_to_leveldb *.dSYM
