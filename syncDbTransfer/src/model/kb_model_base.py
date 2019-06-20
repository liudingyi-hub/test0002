import time
import re


class KbModelBase(object):

    @staticmethod
    def get_plan_arg_date(data_list, index):
        pat_number_code = re.compile('^(\d{8})\d{4}$')
        date_min = None
        for data in data_list:
            ret = pat_number_code.match(data[index])
            if ret and (date_min is None or ret.group(1) < date_min):
                date_min = ret.group(1)
        if not date_min:
            return time.strftime("%Y%m%d")
        return time.strftime("%Y%m%d", time.localtime(time.mktime(time.strptime(date_min, '%Y%m%d')) - 3600 * 24))