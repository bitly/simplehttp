simplehttp
==========

`simplehttp` is a family of libraries and daemons built upon libevent that make high performance HTTP servers 
simple and straightforward to write.

The following libraries and daemons are included:
 
 * `host_pool` - a library for dealing with endpoint selection, pooling, failure, recovery, and backoff
 * `ps_to_http` - a daemon built on top of pubsubclient to write messages from a source pubsub to destination simplequeue or pubsub server
 * `ps_to_file` - a daemon built on top of pubsubclient to write messages from a source pubsub to time rolled output files
 * `pubsub` - a daemon that receives data via HTTP POST events and writes to all subscribed long-lived HTTP connections
 * `pubsub_filtered` - a pubsub daemon with the ability to filter/obfuscate fields of a JSON message
 * `pubsubclient` - a library for writing clients that read from a pubsub
 * `pysimplehttp` - a python library for working with pubsub and simplequeue
 * `qrencode`
 * `queuereader` - a library for writing clients that read from a simplequeue and do work
 * `simpleattributes`
 * `simplegeo`
 * `simplehttp`
 * `simpleleveldb` - a HTTP CRUD interface to leveldb
 * `simplememdb` - an in-memory version of simpletokyo
 * `simplequeue` - an in memory queue with HTTP /put and /get endpoints to push and pop data
 * `simpletokyo` - a HTTP CRUD interface to front tokyo cabinet's ttserver
 * `sortdb` - sorted database server

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
* [leveldb](http://code.google.com/p/leveldb/)
* [tokyocabinet](http://fallabs.com/tokyocabinet/) / [tokyotyrant](http://fallabs.com/tokyotyrant/)
* [qrencode](http://fukuchi.org/works/qrencode/index.en.html)
* [pcre](http://www.pcre.org/)

pysimplehttp Install Instructions
=================================

    pip install pysimplehttp

provides `file_to_sq.py` and `ps_to_sq.py`. It will use ujson if available.
