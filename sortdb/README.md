sortdb
======

Sorted database server. Makes a tab (or comma) delimitated sorted file accessible via HTTP

	OPTIONS
	
	--address=<str>        address to listen on
	                       default: 0.0.0.0
	--daemon               daemonize process
	--db-file=<str>       
	--enable-logging       request logging
	--field-separator=<char> field separator (eg: comma, tab, pipe). default: TAB
	--group=<str>          run as this group
	--help                 list usage
	--port=<int>           port to listen on
	                       default: 8080
	--root=<str>           chdir and run from this directory
	--user=<str>           run as this user

API endpoints:

 * /get?key=...   
    
 * /mget?k=&k=...   

 * /stats
 
 * /reload (reload/remap the db file)
 
 * /exit (cause the current process to exit)

a HUP signal will also cause sortdb to reload/remap the db file
