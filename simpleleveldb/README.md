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

```
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
```

API endpoints:

 * /get

    parameters: `key`

 * /mget

    parameters: `key` (multiple)

 * /fwmatch

    parameters: `key`, `limit` (default 500)

 * /range_match

    parameters: `start`, `end`, `limit` (default 500)

 * /put

    parameters: `key`, `value`

    Note: `value` can also be specified as the raw POST body content

 * /mput

    Note: takes separator-separated key/value pairs in separate lines in the POST body

 * /list_append

    parameters: `key`, `value` (multiple)

 * /list_prepend

    parameters: `key`, `value` (multiple)

 * /list_remove

    parameters: `key`, `value` (multiple)

 * /list_pop

    parameters: `key`, `position` (default 0), `count` (default 1)

    Note: a negative position does a reverse count from the end of the list

 * /set_add

    parameters: `key`, `value` (multiple)

 * /set_remove

    parameters: `key`, `value` (multiple)

 * /set_pop

    parameters: `key`, `count` (default 1)

 * /dump_csv

    parameters: `key` (optional)

    Note: dumps the entire database starting at `key` or else at the beginning, in txt format csv

 * /del

    parameters: `key`

 * /stats

 * /exit

    Note: causes the process to exit

All endpoints take a `format` parameter which affects whether error conditions
are represented by the HTTP response code (format=txt) or by the "status_code"
member of the json result (format=json) (in which case the HTTP response code
is always 200 if the server isn't broken). `format` also affects the output
data for all endpoints except /put, /mput, /exit, /del, and /dump_csv.

Output data in json format is under the "data" member of the root json object,
sometimes as a string (/get), sometimes as an array (/mget), sometimes as an
object with some metadata (/list_remove).

Most endpoints take a `separator` parameter which defaults to "," (but can be
set to any single character), which affects txt format output data. It also
affects the deserialization and serialization of lists and sets stored in the
db, and the input parsing of /mput.

All list and set endpoints take a `return_data` parameter; set it to 1 to additionally
return the new value of the list or set. However, this doesn't work for list_pop
or set_pop endpoints in txt format.

Utilities
---------

* `leveldb_to_csv` is a utility to dump a leveldb database into csv format. It takes the same parameters as simpleleveldb plus an optional `--output-file` and `--output_deliminator`  (or run `--help` for more info)
* `csv_to_leveldb` loads from a csv into a leveldb database. It takes the same parameters as simpleleveldb plus an optional `--input-file` and `--input_deliminator`  (or run `--help` for more info)
