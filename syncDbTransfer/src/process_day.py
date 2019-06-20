import logging
import traceback

from processor import Processor
from statistic_day import statistic_device_full_rate
from statistic_day import statistic_device_utilization
from statistic_day import statistic_total_pot_statistic


class DayProcess(Processor):

    def __init__(self):
        super(DayProcess, self).__init__(interval=3600)

    def task(self, func, *args, **kwargs):
        try:
            func(*args, **kwargs)
        except Exception as err:
            logging.getLogger('transfer').debug(traceback.format_exc())
            logging.getLogger('transfer').error("cals %s error %s" % (function, str(err)))
            pass

    def process(self):
        self.task(statistic_device_utilization)
        self.task(statistic_device_full_rate)
        self.task(statistic_total_pot_statistic)




