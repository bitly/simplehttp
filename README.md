simplehttp
==========

simplehttp is a library built upon libevent that makes high performance http based servers simple to write.

The following daemons are built on simplehttp and included

 * pubsub - a daemon that receives data via http POST events and writes that data to all currently connected long-lived http connections
 * pubsub_to_pubsub - a library for piping data from one pubsub stream to another pubsub server
 * simplequeue - an in memory queue with HTTP /get and /post endpoints to push/pop data
 * simpletokyo - a HTTP /get /post /del /fwmatch /incr interface in front of ttserver
 * sortdb - Sorted database server
 * simplegeo
 * simplememdb - an in-memory version of simpletokyo
 * qrencode

simplehttp Install Instructions
===============================

to install any of the simplehttp components you will need to install 
[libevent](http://www.monkey.org/~provos/libevent/) 1.4.13+ and the 'simplehttp' module first.

build the main library
this provides libsimplehttp.a simplehttp/simplehttp.h and simplehttp/queue.h

    cd simplehttp
    make && make install

now install whichever module you would like
this will compile 'simplequeue' and place it in /usr/local/bin

    cd simplequeue
    make && make install

Some modules have additional dependencies:

* [json-c](http://oss.metaparadigm.com/json-c/)
* [tokyocabinet](http://fallabs.com/tokyocabinet/) / [tokyotyrant](http://fallabs.com/tokyotyrant/)
* [qrencode](http://fukuchi.org/works/qrencode/index.en.html)
* [pcre](http://www.pcre.org/)

pysimplehttp Install Instructions
=================================

    pip install pysimplehttp

