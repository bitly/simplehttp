"""
Simplequeue base reader class.

This does a /get on a simplequeue and calls task methods to process that message

It handles the logic for backing off on retries and giving up on a message

"""
import datetime
import sys
import logging
import os
import tempfile
import time
import copy
import tornado.options
import signal

import http
import BackoffTimer
from host_pool import HostPool

try:
    import ujson as json
except ImportError:
    import json

tornado.options.define('heartbeat_file', type=str, default=None, help="path to a file to touch for heartbeats every 10 seconds")


class RequeueWithoutBackoff(Exception):
    """exception for requeueing a message without incrementing backoff"""
    pass


class BaseReader(object):
    def __init__(self, simplequeue_address, all_tasks, max_tries=5, sleeptime_failed_queue=5,
            sleeptime_queue_empty=0.5, sleeptime_requeue=1, requeue_delay=90, mget_items=0,
            failed_count=0, queuename=None, preprocess_method=None, validate_method=None,
            requeue_giveup=None, failed_message_dir=None):
        """
        BaseReader provides a queue that calls each task provided by ``all_tasks`` up to ``max_tries``
        requeueing on any failures with increasing multiples of ``requeue_delay`` between subsequent
        tries of each message.

        ``preprocess_method`` defines an optional method that can alter the message data before
        other task functions are called.
        ``validate_method`` defines an optional method that returns a boolean as to weather or not
        this message should be processed.
        ``all_tasks``  defines the a mapping of tasks and functions that individually will be called
        with the message data.
        ``requeue_giveup`` defines a callback for when a message has been called ``max_tries`` times
        ``failed_message_dir`` defines a directory where failed messages should be written to
        """
        assert isinstance(all_tasks, dict)
        for key, method in all_tasks.items():
            assert callable(method), "key %s must have a callable value" % key
        if preprocess_method:
            assert callable(preprocess_method)
        if validate_method:
            assert callable(validate_method)
        assert isinstance(queuename, (str, unicode))
        assert isinstance(mget_items, int)
        
        if not isinstance(simplequeue_address, HostPool):
            if isinstance(simplequeue_address, (str, unicode)):
                simplequeue_address = [simplequeue_address]
            assert isinstance(simplequeue_address, (list, set, tuple))
            simplequeue_address = HostPool(simplequeue_address)
        
        self.simplequeue_address = simplequeue_address
        self.max_tries = max_tries
        self.requeue_giveup = requeue_giveup
        self.mget_items = mget_items
        self.sleeptime_failed_queue = sleeptime_failed_queue
        self.sleeptime_queue_empty = sleeptime_queue_empty
        self.sleeptime_requeue = sleeptime_requeue
        self.requeue_delay = requeue_delay  # seconds
        ## max delay time is requeue_delay * (max_tries + max_tries-1 + max_tries-2 ... 1)
        self.failed_message_dir = failed_message_dir or tempfile.mkdtemp()
        assert os.access(self.failed_message_dir, os.W_OK)
        self.failed_count = failed_count
        self.queuename = queuename
        self.task_lookup = all_tasks
        self.preprocess_method = preprocess_method
        self.validate_method = validate_method
        self.backoff_timer = dict((k, BackoffTimer.BackoffTimer(0, 120)) for k in self.task_lookup.keys())
        # a default backoff timer
        self.backoff_timer['__preprocess'] = BackoffTimer.BackoffTimer(0, 120)
        self.quit_flag = False

    def callback(self, queue_message):
        # copy the dictionary, dont reference
        message = copy.copy(queue_message.get('data', {}))

        try:
            if self.preprocess_method:
                message = self.preprocess_method(message)

            if self.validate_method and not self.validate_method(message):
                self.backoff_timer['__preprocess'].success()
                return
        except:
            logging.exception('caught exception while preprocessing')
            self.backoff_timer['__preprocess'].failure()
            return self.requeue(queue_message)

        self.backoff_timer['__preprocess'].success()

        for task in copy.copy(queue_message['tasks_left']):
            method = self.task_lookup[task]
            try:
                if method(message):
                    queue_message['tasks_left'].remove(task)
                    self.backoff_timer[task].success()
                else:
                    self.backoff_timer[task].failure()
            except RequeueWithoutBackoff:
                logging.info('RequeueWithoutBackoff')
            except:
                logging.exception('caught exception while handling %s' % task)
                self.backoff_timer[task].failure()

        if queue_message['tasks_left']:
            self.requeue(queue_message)

    def simplequeue_get(self):
        try:
            simplequeue_addr = self.simplequeue_address.get()
            if self.mget_items:
                msg = http.http_fetch(simplequeue_addr + '/mget?items=' + str(self.mget_items))
            else:
                msg = http.http_fetch(simplequeue_addr + '/get')
            self.simplequeue_address.success(simplequeue_addr)
            return msg
        except:
            self.simplequeue_address.failed(simplequeue_addr)
            raise

    def simplequeue_put(self, data):
        try:
            simplequeue_addr = self.simplequeue_address.get()
            http.http_fetch(simplequeue_addr + '/put', dict(data=data))
            self.simplequeue_address.success(simplequeue_addr)
        except:
            self.simplequeue_address.failed(simplequeue_addr)
            raise

    def dump(self, message):
        self.failed_count += 1
        path = os.path.join(self.failed_message_dir, self.queuename)
        if not os.path.exists(path):
            os.makedirs(path)
        date_str = datetime.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        filename = "%s.%d.json" % (date_str, self.failed_count)
        f = open(os.path.join(path, filename), 'wb')
        if isinstance(message, (str, unicode)):
            f.write(message)
        else:
            f.write(json.dumps(message))
        f.close()

    def requeue(self, message, delay=True, requeue_delay=None):
        """
        requeue this message incrementing the retry_on, and incrementing the tries when delay=True
        if delay=False, just put it back in the queue as it's not time to run it yet
        """
        if message['tries'] > self.max_tries:
            logging.warning('giving up on message after max tries %s' % str(message))
            try:
                if self.requeue_giveup != None:
                    self.requeue_giveup(message)
            except Exception, e:
                logging.exception("Could not call requeue_giveup callback: %s"%e)
            self.dump(message)
            return

        if delay:
            ## delay the next try
            if requeue_delay is None:
                requeue_delay = self.requeue_delay * message['tries']
            message['retry_on'] = time.time() + requeue_delay

        if message['retry_on']:
            next_try_in = message['retry_on'] - time.time()
        else:
            next_try_in = 0

        try:
            self.simplequeue_put(json.dumps(message))
            logging.info('requeue(%s) next try in %d secs' % (str(message), next_try_in))
        except:
            logging.exception('requeue(%s) failed' % str(message))
        time.sleep(self.sleeptime_requeue)

    def handle_message(self, message_bytes):
        try:
            message = json.loads(message_bytes)
        except:
            logging.warning('invalid data: %s' % str(message_bytes))
            self.dump(message_bytes)
            return

        if not message.get('data'):
            # wrap in the reader params
            message = {
                'data': message,
                'tries': 0,
                'retry_on': None,
                'started': int(time.time())
            }

        # add tasks_left so it's possible for someone else to add a queue entry
        # with the metadata wrapper (ie: to queue for replay later) but without
        # knowledge of the tasks that need to happen in *this* queue reader
        if 'tasks_left' not in message:
            message['tasks_left'] = self.task_lookup.keys()

        # should we wait for this?
        retry_on = message.get('retry_on')
        if retry_on and retry_on > int(time.time()):
            self.requeue(message, delay=False)
            return

        message['tries'] = message['tries'] + 1
        logging.info('handling %s' % str(message))
        self.callback(message)
        self.end_processing_sleep()

    def update_heartbeat(self):
        heartbeat_file = tornado.options.options.heartbeat_file
        if not heartbeat_file:
            return
        now = time.time()
        heartbeat_update_interval = 10
        # update the heartbeat file every N seconds
        if not hasattr(self, '_last_heartbeat_update'):
            self._last_heartbeat_update = now - heartbeat_update_interval - 1

        if self._last_heartbeat_update < now - heartbeat_update_interval:
            self._last_heartbeat_update = now
            open(heartbeat_file, 'a').close()
            os.utime(heartbeat_file, None)

    def end_processing_sleep(self):
        interval = max(bt.get_interval() for i, bt in self.backoff_timer.iteritems())
        if interval > 0:
            logging.info('backing off for %0.2f seconds' % interval)
            time.sleep(interval)

    def handle_term_signal(self, sig_num, frame):
        logging.info('TERM Signal handler called with signal %r.' % sig_num)
        if self.quit_flag:
            # if we call the term signal twice, just exit immediately
            logging.warning('already wating for exit flag, so aborting')
            sys.exit(1)
        self.quit_flag = True

    def run(self):
        signal.signal(signal.SIGTERM, self.handle_term_signal)
        logging.info("starting %s reader..." % self.queuename)
        while not self.quit_flag:
            try:
                self.update_heartbeat()
            except:
                logging.exception('failed touching heartbeat file')
            
            try:
                message_bytes = self.simplequeue_get()
            except:
                logging.exception('queue.get() failed')
                time.sleep(self.sleeptime_failed_queue)
                continue
            
            if not message_bytes:
                time.sleep(self.sleeptime_queue_empty)
                continue

            if self.mget_items:
                messages = message_bytes.splitlines()
            else:
                messages = [message_bytes]

            for message in messages:
                try:
                    self.handle_message(message)
                except:
                    logging.exception('failed to handle_message() %r' % message)
                    return

