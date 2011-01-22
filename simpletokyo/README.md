simpletokyo
===========

SimpleTokyo provides a light http interface to [Tokyo Tyrant](http://fallabs.com/tokyotyrant/). It supports
the following commands.

Cmdline usage:

    -A 127.0.0.1 (ttserver host to connect to)
    -P 1978 (ttserver port to connect to)
    -a 0.0.0.0 (address to bind to)
    -p 8080 (port to bind to)
    -D daemonize (default off)

API endpoints:

 * /get
  parameter: key
 
 * /put
  parameter: key
  parameter: value
 
 * /del
  parameter: key
 
 * /fwmatch
  parameter:key
  parameter:max (optional)
  parameter:length (optional)
  parameter:offset (optional)

 * /incr
  parameter:key
  parameter:value

 * /get_int (to return values added with /incr)
  parameter:key

Dependencies

 * [libevent](http://monkey.org/~provos/libevent/) 1.4.13+
 * [json-c](http://oss.metaparadigm.com/json-c/)
 * [tokyocabinet](http://fallabs.com/tokyocabinet/) / [tokyotyrant](http://fallabs.com/tokyotyrant/)
