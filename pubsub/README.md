pubsub
======

pubsub (short for publish/subscribe) is a server that brokers new messages 
to all connected subscribers at the time a message is received.
http://en.wikipedia.org/wiki/Publish/subscribe

Commandline options:

    --address=<str>        address to listen on
                           default: 0.0.0.0
    --daemon               daemonize process
    --enable-logging       request logging
    --group=<str>          run as this group
    --help                 list usage
    --port=<int>           port to listen on
                           default: 8080
    --root=<str>           chdir and run from this directory
    --user=<str>           run as this user
    --version              

API endpoints:

 * /pub   
  parameter: body
 
 * /sub  
  request parameter: multipart=(1|0). turns on/off chunked response format (on by default)
  long lived connection which will stream back new messages.
  
 * /stats
  request parameter: reset=1 (resets the counters since last reset) 
  response: Active connections, Total connections, Messages received, Messages sent, Kicked clients.
  
 * /clients
  response: list of remote clients, their connect time, and their current outbound buffer size.
