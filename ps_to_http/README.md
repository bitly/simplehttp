ps_to_http
==========

helper application to subscribe to a pubsub and write incoming messages
to simplequeue(s).

supports multiple destination simplequeues via round robin.

source pubsub should output non-multipart, chunked data where each 
message is newline terminated.

OPTIONS
-------
```
  --destination-get-url=<str> (multiple) url(s) to HTTP GET to
                         This URL must contain a %s for the message data
                         for a simplequeue use "http://127.0.0.1:8080/put?data=%s"
  --destination-post-url=<str> (multiple) url(s) to HTTP POST to
                         For a pubsub endpoint use "http://127.0.0.1:8080/pub"
  --help                 list usage
  --pubsub-url=<str>     url of pubsub to read from
                         default: http://127.0.0.1:80/sub?multipart=0
  --round-robin          write round-robin to destination urls
  --max-silence          Maximum amount of time (in seconds) between messages from
                         the source pubsub before quitting.
```
