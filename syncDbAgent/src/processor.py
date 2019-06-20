
import time
import os
import logging
import traceback


class Processor(object):
    def __init__(self, interval=60):
        self._stop = False
        self._interval = interval

    def stop(self):
        self._stop = True

    def run(self):
        while not self._stop:
            try:
                self.process()
            except Exception as e:
                logging.getLogger('agent').error(traceback.format_exc())
                logging.getLogger('agent').error("%s meet error: %s when process" % (self.__class__.__name__, e))
            time.sleep(self._interval)

    def process(self):
        pass