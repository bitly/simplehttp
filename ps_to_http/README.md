ps_to_http
======

helper application to subscribe to a pubsub and write incoming messages
to simplequeue(s).

supports multiple destination simplequeues via round robin.

source pubsub should output non-multipart, chunked data where each 
message is newline terminated.

Commandline Options:

    --pubsub-url=<str>          source pubsub url in the form of 
                                    http://domain.com:port/path
    --simplequeue-url=<str>     destination simplequeue url in the form of 
                                    http://domain.com:port/ (multiple)
    --version
