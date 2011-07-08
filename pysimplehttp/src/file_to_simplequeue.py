import tornado.ioloop
import tornado.httpclient
import os
import functools
import gzip
import ujson as json
import logging
import urllib

class FileToSimplequeue(object):
    http = tornado.httpclient.AsyncHTTPClient(max_simultaneous_connections=50, max_clients=50)
    def __init__(self, input_file, max_concurrent, max_queue_depth, simplequeue_urls, check_simplequeue_interval, stats_interval):
        assert isinstance(simplequeue_urls, (list, tuple))
        assert isinstance(max_queue_depth, int)
        assert isinstance(max_concurrent, int)
        assert isinstance(check_simplequeue_interval, int)
        assert isinstance(stats_interval, int)
        for entry in simplequeue_urls: 
            assert entry.startswith("http://") or entry.startswith("https://"), "simplequeue url %s is not valid" % entry
        self.simplequeue_urls = simplequeue_urls
        self.input = self.open_file(input_file)
        self.concurrent = 0
        self.finished = False
        self.fill_check = False
        self.max_queue_depth = max_queue_depth
        self.max_concurrent = max_concurrent
        self.check_simplequeue_interval
        self.pending = dict([[simplequeue, 0] for simplequeue in simplequeue_urls])
        self.success = 0
        self.failed = 0
        self.stats_interval = stats_interval
    
    def stats_incr(self, successful=True):
        if successful:
            self.success += 1
        else:
            self.failed += 1

    def stats_reset(self):
        self.success = 0
        self.failed = 0
    
    def print_and_reset_stats(self):
        logging.warning('success: %5d failed: %5d concurrent: %2d' % (self.success, self.failed, self.concurrent))
        self.stats_reset()
    
    def start(self):
        self.stats_timer = tornado.ioloop.PeriodicCallback(self.print_and_reset_stats, self.stats_interval * 1000)
        self.stats_timer.start()
        self.check_timer = tornado.ioloop.PeriodicCallback(self.check_simplequeue_depth, self.check_simplequeue_interval * 1000)
        self.check_timer.start()
        self.check_simplequeue_depth() # seed the loop
        tornado.ioloop.IOLoop.instance().start()
        
    def open_file(self, filename):
        assert os.path.exists(filename), "%r is not accessible" % filename
        if filename.endswith('.gz'):
            return gzip.open(filename, 'rb')
        else:
            return open(filename, 'rb')
    
    def check_simplequeue_depth(self):
        """query the simplequeue and fill it based on where it's dept should be"""
        if self.finished:
            return self.finish()
        for simplequeue in self.simplequeue_urls:
            self.http.fetch(simplequeue + '/stats?format=json', 
                callback=functools.partial(self.finish_check_simplequeue_depth, simplequeue=simplequeue))

    def finish_check_simplequeue_depth(self, response, simplequeue):
        stats = json.loads(response.body)
        entries_needed = self.max_queue_depth - stats['depth']
        entries_needed = max(0, entries_needed)
        logging.info('%s needs %d entries' % (simplequeue, entries_needed))
        self.pending[simplequeue] = entries_needed
        if not self.fill_check:
            self.fill_check = True
            tornado.ioloop.IOLoop.instance().add_callback(self.fill_as_needed)

    def fill_as_needed(self):
        """
        as needed based on how many more should go in a simplequeue, and the current concurrency
        """
        self.fill_check = False
        if self.finished:
            return self.finish()
        available_concurrency = self.max_concurrent - self.concurrent
        for simplequeue in self.pending.keys():
            while available_concurrency and self.pending[simplequeue] > 0:
                available_concurrency -= 1
                self.pending[simplequeue] -= 1
                self.fill_one(simplequeue)

    def fill_one(self, endpoint):
        """read one line from `self.input` and send it to a simplequeue"""
        line = self.input.readline()
        if not line:
            return self.finish()
        self.concurrent += 1
        url = endpoint + '/put?' + urllib.urlencode(dict(data=line))
        self.http.fetch(url, self.finish_fill_one)

    def finish_fill_one(self, response):
        self.concurrent -= 1
        if response.code != 200:
            logging.info(response)
            self.stats.failed += 1
        else:
            self.stats.success += 1
    
        # continue loop
        if self.max_concurrent > self.concurrent and not self.fill_check:
            self.fill_check = True
            tornado.ioloop.IOLoop.instance().add_callback(self.fill_as_needed)

    def finish(self):
        self.finished = True
        if self.concurrent == 0:
            logging.info('stopping ioloop')
            tornado.ioloop.IOLoop.instance().stop()
