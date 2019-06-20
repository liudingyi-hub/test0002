# coding: utf-8
import time
import pymongo
import logging
from datetime import datetime, timedelta

import settings


def isVaildDate(date_):
    try:
        time.strptime(date_, "%Y%m%d")
        return True
    except Exception as e:
        return False


def  get_process_id(id):
    try:
        id = str(int(id))
    except:
        return None
    process_id = ''
    if len(id) >= 6:
        num = id[-3:]
        day = id[-5:-3]
        if len(id) >= 7:
            month = id[-7:-5]
        else:
            month = '0'+id[-6:-5]
        y = str(datetime.now().year)
        date_ = y+month+day
        if isVaildDate(date_):
            process_id = '1'+y[-2:]+month+day+num
        else:
            return None
    else:
        return None
    result = None
    try:
        result = int(process_id)
    except:
        pass
    return result


def get_device_id(id, pre='01001'):
    str_id = str(id)
    while 1:
        if len(str_id) < 4:
            str_id = '0' + str_id
        else:
            break
    return pre+str_id


class DBManager(object):
    def __init__(self, db_name, table_name, host='', port=0):
        if host:
            self.host = host
        else:
            self.host = settings.MONGO_HOST
        if port:
            self.port = port
        else:
            self.port = settings.MONGO_PORT
        self.cursor = None
        self.db_name = db_name
        self.table_name = table_name
        self.__collection()

    def __collection(self):
        if self.cursor is None:
            # logging.getLogger('transfer').info("host:%s port:%s" % (self.host, self.port))
            client = pymongo.MongoClient(self.host, self.port)
            self.cursor = client[self.db_name][self.table_name]

    def __add_time(self, data):
        for item in data:
            item['insert_time'] = datetime.now()

    def __add_time_item(self, item):
        item['insert_time'] = datetime.now()

    def update_data(self, keys, data):
        """
        :param filter: 用于定位数据，是一个dict
        :param data: 用于更新filter找到的所有数据，是一个dict
        :return:
        """
        if not data:
            return
        filter = {}
        for key in keys:
            filter[key] = data[key]
        self.__add_time_item(data)
        self.cursor.update(filter, {'$set': data}, upsert=True)

    def insert_data(self, data):
        if not data:
            return
        self.__add_time(data)
        logging.getLogger('transfer').info('insert data. %d' % len(data))
        # print('print insert data. %d' % len(data))
        self.cursor.insert_many(data, ordered=True)

    def insert_unique(self, keys, data):
        if not data:
            return
        i_data = []
        for item in data:
            filter = {}
            for key in keys:
                filter[key] = item[key]
            if not self.find(filter).count():
                i_data.append(item)
        logging.getLogger('transfer').info('insert data. %d' % len(i_data))
        self.insert_data(i_data)

    def find(self, filter=None):
        if filter:
            return self.cursor.find(filter)
        else:
            return self.cursor.find()

    def find_sort(self, filter, rule_):
        return self.cursor.find(filter).sort(rule_)

    def find_one(self, filter=None):
        if filter:
            return self.cursor.find_one(filter)
        else:
            return self.cursor.find_one()

    def distinct(self, field):
        return self.cursor.distinct(field)

    def remove_all(self):
        return self.cursor.remove({})

    def delete(self, filter):
        return self.cursor.delete_many(filter)

    def delete_days(self, day):
        return self.cursor.delete_many({'timestamp': {'$lt': datetime.now()-timedelta(days=day)}})
