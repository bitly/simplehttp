pubsub_filtered
===============

pubsub_filtered connects to a remote pubsub server and filters out or 
hashes entries before re-publishing as a pubsub stream

if you have a message like '{desc:"ip added", ip:"127.0.0.1"}' to encrypt the ip you would start pubsub_filtered with '-e ip'

OPTIONS

```
  --address=<str>        address to listen on
                         default: 0.0.0.0
  --blacklist-fields=<str> comma separated list of fields to remove
  --daemon               daemonize process
  --enable-logging       request logging
  --encrypted-fields=<str> comma separated list of fields to encrypt
  --expected-key=<str>   key to expect in messages before echoing to clients
  --expected-value=<str> value to expect in --expected-key field in messages before echoing to clients
  --group=<str>          run as this group
  --help                 list usage
  --port=<int>           port to listen on
                         default: 8080
  --pubsub-url=<str>     url of pubsub to read from
                         default: http://127.0.0.1/sub?multipart=0
  --root=<str>           chdir and run from this directory
  --user=<str>           run as this user
  --version              
```

API endpoints:

 * /sub?filter_subject=x&filter_pattern=^a 
  long lived connection which will stream back new messages; one per line.
  filter_subject (optional): the filter key, exact match
  filter_pattern (optional): the filter pattern, pcre
  
 * /stats
  response: Active connections, Total connections, Messages received, Messages sent, Kicked clients, upstream reconnect.
  
 * /clients
  response: list of remote clients, their connect time, and their current outbound buffer size.
