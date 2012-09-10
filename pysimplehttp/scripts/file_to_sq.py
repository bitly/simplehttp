#!/usr/bin/env python
import tornado.options
import os
import sys
import pysimplehttp.file_to_simplequeue

if __name__ == "__main__":
    tornado.options.define("input_file", type=str, default=None, help="File to load")
    tornado.options.define("max_queue_depth", type=int, default=2500, help="only fill the queue to DEPTH entries")
    tornado.options.define("simplequeue_url", type=str, multiple=True, help="(multiple) url(s) for simplequeue to write to")
    tornado.options.define("stats_interval", type=int, default=60, help="seconds between displaying stats")
    tornado.options.define("concurrent_requests", type=int, default=20, help="number of simultaneous requests")
    tornado.options.define("check_simplequeue_interval", type=int, default=1, help="seconds between checking simplequeue depth")
    tornado.options.define('filter_require', type=str, multiple=True, help="filter json message to require for key=value")
    tornado.options.define('filter_exclude', type=str, multiple=True, help="filter json message to exclude for key=value")
    tornado.options.parse_command_line()
    
    options = tornado.options.options
    if not options.input_file or not os.path.exists(options.input_file):
        sys.stderr.write("ERROR: --input_file=%r does not exist\n" % options.input_file)
        sys.exit(1)
    if not options.simplequeue_url:
        sys.stderr.write('ERROR: --simplequeue_url required\n' )
        sys.exit(1)

    file_to_sq = pysimplehttp.file_to_simplequeue.FileToSimplequeue(options.input_file, 
                options.concurrent_requests, 
                options.max_queue_depth, 
                options.simplequeue_url, 
                options.check_simplequeue_interval,
                options.stats_interval,
                filter_require=options.filter_require,
                filter_exclude=options.filter_exclude,
                )
    file_to_sq.start()

    
