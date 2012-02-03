TARGET ?= /usr/local
LIBEVENT ?= /usr/local

CFLAGS += -I. -I$(LIBEVENT)/include -g -Wall -O2
LIBS = -L. -L$(LIBEVENT)/lib -L/usr/local/lib -levent -ldomain_socket
AR = ar
AR_FLAGS = rc
RANLIB = ranlib

all: libdomain_socket.a demo
    
libdomain_socket.a: domain_socket.o
	/bin/rm -f $@
	$(AR) $(AR_FLAGS) $@ $^
	$(RANLIB) $@

demo: demo.c
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS)

install:
	/usr/bin/install -d $(TARGET)/lib/
	/usr/bin/install -d $(TARGET)/bin/
	/usr/bin/install -d $(TARGET)/include/domain_socket
	/usr/bin/install libdomain_socket.a $(TARGET)/lib/
	/usr/bin/install domain_socket.h $(TARGET)/include/domain_socket

clean:
	/bin/rm -f *.a *.o
