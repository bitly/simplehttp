ps_to_file
======

helper application to subscribe to a pubsub and write incoming messages
to time rolled files.

source pubsub should output non-multipart, chunked data where each 
message is newline terminated.

OPTIONS
-------

```
    --pubsub-url=<str>          source pubsub url in the form of 
                                    http://domain.com:port/path
    --filename-format=<str>     output filename format (strftime compatible)
                                    /var/log/pubsub.%%Y-%%m-%%d_%%H.log
    --version
```