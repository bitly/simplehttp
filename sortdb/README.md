sortdb
======

Sorted database server. Makes a tab (or comma) delimitated sorted file accessible via HTTP

Cmdline usage:

    -f /path/to/dbfile
    -F "\t" (field deliminator)
    -a 127.0.0.1 (address to listen on)
    -p 8080 (port to listen on)
    -D (daemonize)

API endpoints:

 * /get?key=...   
    
 * /stats

a HUP signal will cause sortdb to reload/remap the db file