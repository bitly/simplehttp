pubsub
======

pubsub (short for publish/subscribe) is a server that brokers new messages 
to all connected subscribers at the time a message is received.
http://en.wikipedia.org/wiki/Publish/subscribe

Cmdline usage:

    -a 0.0.0.0 (address to bind to)
    -p 8080 (port to bind to)
    -D daemonize (default off)

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
