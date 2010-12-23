import logging
import tornado.options
import pubsub_reader

class MyReader(pubsub_reader.PubsubReader):
    def callback(self, data):
        """handle each chunk of data from the pubsub server"""
        logging.info(data)

if __name__ == "__main__":
    tornado.options.parse_command_line()
    reader = MyReader();
    reader.open('127.0.0.1', 80)
    reader.io_loop.start()
