simpleleveldb
=============

HTTP based leveldb server. 

Building
--------

you need a copy of leveldb

    cd /tmp/
    git clone https://code.google.com/p/leveldb/
    cd leveldb
    make
    cp libleveldb.a /usr/local/lib/
    cp -r include/leveldb /usr/local/include/

then in simpleleveldb

    env LIBLEVELDB=/usr/local make
    make install

OPTIONS
-------
  --address=<str>        address to listen on
                         default: 0.0.0.0
  --block-size=<int>     block size
                         default: 4096
  --compression=True|False snappy compression
  --create-db-if-missing=True|False Create leveldb file if missing
  --daemon               daemonize process
  --db-file=<str>        path to leveldb file
  --enable-logging       request logging
  --error-if-db-exists   Error out if leveldb file exists
  --group=<str>          run as this group
  --help                 list usage
  --leveldb-max-open-files=<int> leveldb max open files
                         default: 4096
  --paranoid-checks=True|False leveldb paranoid checks
  --port=<int>           port to listen on
                         default: 8080
  --root=<str>           chdir and run from this directory
  --user=<str>           run as this user
  --version              0.1
  --write-buffer-size=<int> write buffer size
                         default: 4194304

API endpoints:

 * /get
 
    parameters: `key`, `format`
    
 * /mget

    parameters: `key` (multiple), `format`

 * /put

    parameters: `key`, `value`, `format`

 * /del

    parameters: `key`, `format`

 * /stats
 
 * /exit (cause the current process to exit)
