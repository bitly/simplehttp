pubsub_filtered_
======

pubsub_filtered connects to a remote pubsub server and filters out or 
hashes entries before re-publishing as a pubsub stream

if you have a message like '{desc:"ip added", ip:"127.0.0.1"}' to encrypt the ip you would start pubsub_filtered with '-e ip'

Cmdline Usage: (these options must come before normal pubsub server options)

    -s host:port
    -e keys,to,hash
    -k keys,to,excude

API endpoints:

 * /sub  
  long lived connection which will stream back new messages; one per line.
  
 * /stats
  response: Active connections, Total connections, Messages received, Messages sent, Kicked clients, upstream reconnect.
  
 * /clients
  response: list of remote clients, their connect time, and their current outbound buffer size.
