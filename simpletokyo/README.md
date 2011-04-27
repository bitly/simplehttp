simpletokyo
===========

SimpleTokyo provides a light http interface to [Tokyo Tyrant](http://fallabs.com/tokyotyrant/). It supports
the following commands.

Command Line Options:

  --address=<str>        address to listen on
                         default: 0.0.0.0
  --daemon               daemonize process
  --enable-logging       request logging
  --group=<str>          run as this group
  --help                 list usage
  --port=<int>           port to listen on
                         default: 8080
  --root=<str>           chdir and run from this directory
  --ttserver-host=<str> 
                         default: 127.0.0.1
  --ttserver-port=<int> 
                         default: 1978
  --user=<str>           run as this user
  --version              

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
  parameter:format=json|txt [default json]

 * /fwmatch_int (returns values added with /incr)
  parameter:key
  parameter:max (optional)
  parameter:length (optional)   
  parameter:offset (optional)   
  parameter:format=json|txt [default json]   

 * /fwmatch_int_merged (returns values added with /incr via prefix merge to single lines)
  parameter:key
  parameter:max (optional)
  parameter:length (optional)   
  parameter:offset (optional)   
  parameter:format=json|txt [default json]   

 * /incr   
  parameter:key   
  parameter:value   

 * /get_int (to return values added with /incr)   
  parameter:key (accepts multiple &key= parameters)

 * /mget_int (to return values added with /incr)   
  parameter:key (accepts multiple &key= parameters)

 * /mget
  parameter:key (accepts multiple &key= parameters)
  parameter:format=json|txt

 * /vanish (empty the database)

 * /stats (example output)   
  parameter: format=json (optional)   
     Total requests: 439   
     /get requests: 0   
     /get_int requests: 0   
     /put requests: 303   
     /del requests: 136   
     /fwmatch requests: 0   
     /fwmatch_int_ requests: 0   
     /incr requests: 0   
     /vanish requests: 0   
     db opens: 1

 * /exit 

Dependencies

 * [libevent](http://monkey.org/~provos/libevent/) 1.4.13+
 * [json-c](http://oss.metaparadigm.com/json-c/)
 * [tokyocabinet](http://fallabs.com/tokyocabinet/) / [tokyotyrant](http://fallabs.com/tokyotyrant/)
