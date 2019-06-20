import logging
import traceback

from processor import Processor
from statistic_minute import statistic_device_standby_time, statistic_device_work_pot
from temperature_analysis import write_latest_temperature_analysis


class MinuteProcess(Processor):

    def __init__(self):
        super(MinuteProcess, self).__init__(interval=60)

    def task(self, func, *args, **kwargs):
        try:
            func(*args, **kwargs)
        except Exception as err:
            logging.getLogger('transfer').debug(traceback.format_exc())
            logging.getLogger('transfer').error("cals %s error %s" % (function, str(err)))
            pass

    def process(self):
        self.task(statistic_device_standby_time)
        self.task(statistic_device_work_pot)
        self.task(write_latest_temperature_analysis)




