simplememdb
===========

in-memory http accessible tokyo cabinet database

Cmdline usage:

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

 * /dump   
  parameter:regex (optional regex to dump specific keys)

 * /fwmatch   
  parameter:key   
  parameter:max (optional)   
  parameter:length (optional)   
  parameter:offset (optional)   

  * /fwmatch_int (returns values added with /incr)   
   parameter:key   
   parameter:max (optional)   
   parameter:length (optional)   
   parameter:offset (optional)   

 * /incr   
  parameter:key   
  parameter:value   

 * /get_int (to return values added with /incr)   
  parameter:key (accepts multiple &key= parameters)
 
 * /vanish (empty the database)

 * /stats (example output)   
  parameter: format=json|txt   

 * /exit 

Dependencies

 * [libevent](http://monkey.org/~provos/libevent/) 1.4.13+
 * [json-c](http://oss.metaparadigm.com/json-c/)
 * [tokyocabinet](http://fallabs.com/tokyocabinet/)
 * [pcre](http://www.pcre.org/)
