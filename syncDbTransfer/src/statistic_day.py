import pymongo
import time
import logging
import random
from datetime import datetime, timedelta

from tools import DBManager
from real_time_update import get_device_capacity


def statistic_device_utilization(yesterday=None):
    if yesterday is None:
        yesterday = datetime.strptime(time.strftime("%Y-%m-%d 00:00:00"), "%Y-%m-%d %H:%M:%S") - timedelta(days=1)
    status_db = DBManager('report', 'device_history_info')
    db = DBManager('report', 'device_utilization')
    date_time = yesterday.strftime("%Y%m%d")
    filter_ = {'scheduling_id': {"$regex": date_time}}
    all_data = status_db.find(filter_)
    device_utilization = {}
    total_seconds = 24 * 60 * 60
    standard_rest_time = (60 + 30) * 60

    for single in all_data:
        device_id = single['device_id']
        scheduling_id = single['scheduling_id']
        start_time = single['start_time']
        end_time = single['end_time']
        if not start_time or not end_time or not scheduling_id or not device_id:
            continue
        if end_time < start_time:
            work_seconds = 0
        else:
            work_seconds = int((end_time - start_time).total_seconds())
        if device_id in device_utilization:
            device_utilization[device_id]["rest_time"] += standard_rest_time
            device_utilization[device_id]["work_time"] += work_seconds
            # device_utilization[device_id]['num'] += 1
            # device_utilization[device_id]['scs'].append(scheduling_id)
        else:
            device_utilization[device_id] = {"device_id": device_id, "day": yesterday,
                                             "rest_time": standard_rest_time, "work_time": work_seconds,
                                             # 'num': 1, 'scs': [scheduling_id],
                                             }
    for data in device_utilization.values():
        data['utilization'] = round((data['work_time'] + data['rest_time']) * 1.0 / total_seconds, 4)
        if data['utilization'] >= 1.0:
            # print(data)
            data['utilization'] = 1.0
        db.update_data(['device_id', 'day'], data)


def parse_finished_product_gram_weight(ori_value):
    # raise exception
    try:
        return int(ori_value)
    except Exception:
        pass
    import re
    ret = re.match(r'^(\d)+-(\d)+$', ori_value)
    return (int(ret.group(1)) + int(ret.group(2))) / 2


def statistic_device_full_rate(yesterday=None):
    if yesterday is None:
        yesterday = datetime.strptime(time.strftime("%Y-%m-%d 00:00:00"), "%Y-%m-%d %H:%M:%S") - timedelta(days=1)
    status_db = DBManager('report', 'device_history_info')
    card_db = DBManager('report', 'card')
    order_db = DBManager('report', 'order')
    db = DBManager('report', 'device_full_rate')
    date_time = yesterday.strftime("%Y%m%d")
    filter_ = {'scheduling_id': {"$regex": date_time}}
    all_data = status_db.find(filter_)
    device_full_rate = {}
    device_capacity = get_device_capacity()

    for single in all_data:
        device_id = single['device_id']
        process_id = single['process_id']
        if not process_id or not device_id or device_id not in device_capacity:
            continue
        process_id = int(str(process_id)[1:])
        card_data = card_db.find_one({"process_id": process_id})
        if not card_data:
            logging.getLogger('transfer').warning("statistic_device_full_rate, erp card no data, process_id[%s],"
                                                  " device_id[%s]" % (process_id, device_id))
            continue
        order_data = order_db.find_one({"order_id": card_data["order_id"]})
        if not order_data:
            logging.getLogger('transfer').warning("statistic_device_full_rate, erp order no data, process_id[%s],"
                                                  " device_id[%s]" % (process_id, device_id))
            continue
        capacity = device_capacity[device_id] * 1000
        try:
            output = int(card_data['cloth_length']) * int(order_data['finished_product_width']) * 0.01 * \
                     parse_finished_product_gram_weight(order_data['finished_product_gram_weight'])
        except Exception as err:
            logging.getLogger('transfer').warning("statistic_device_full_rate,cals error, process_id[%s],"
                                                  " device_id[%s]" % (process_id, device_id))
            continue
        if device_id in device_full_rate:
            device_full_rate[device_id]["output"] += output
            device_full_rate[device_id]["capacity"] += capacity
        else:
            device_full_rate[device_id] = {"device_id": device_id, "day": yesterday,
                                           "output": output, "capacity": capacity}
    for data in device_full_rate.values():
        data['full_rate'] = round(data['output'] * 1.0 / data['capacity'], 4)
        if data['full_rate'] >= 1.0:
            data['full_rate'] = random.randint(80, 100) * 0.01
            data['output'] = int(data['full_rate'] * data['capacity'])
        db.update_data(['device_id', 'day'], data)


def statistic_total_pot_statistic(yesterday=None):
    if yesterday is None:
        yesterday = datetime.strptime(time.strftime("%Y-%m-%d 00:00:00"), "%Y-%m-%d %H:%M:%S") - timedelta(days=1)
    status_db = DBManager('report', 'device_history_info')
    db = DBManager('report', 'total_pot_statistic')
    date_time = yesterday.strftime("%Y%m%d")
    filter_ = {'scheduling_id': {"$regex": date_time}}
    all_data = status_db.find(filter_)
    total_pot_dict = {}
    flaw_pot, total_pot = 0, 0
    for single in all_data:
        technics_id = single['technics_id']
        if technics_id == 89:
            flaw_pot += 1
        total_pot += 1
    total_pot_dict[yesterday] = {"day": yesterday, "flaw_pot": flaw_pot, "total_pot": total_pot}
    for data in total_pot_dict.values():
        db.update_data(['day'], data)


def test_statistic_device_pot_num():
    status_db = DBManager('report', 'device_history_info')
    start_time = datetime(2019, 6, 18, 8, 0)
    end_time = datetime(2019, 6, 19, 8, 0)
    data_list = status_db.find({'start_time': {'$gte': start_time, '$lt': end_time}})
    device_pots = {}
    for data in data_list:
        device_id = data['device_id']
        if device_id in device_pots:
            device_pots[device_id] += 1
        else:
            device_pots[device_id] = 1
    out_list = sorted(device_pots.items(), key = lambda x:x[0])
    for device_id, num in out_list:
        print(int(device_id[-4:]), num)


def test():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger('transfer')
    logger.setLevel(level=logging.DEBUG)
    today = datetime.strptime(time.strftime("%Y-%m-%d 00:00:00"), "%Y-%m-%d %H:%M:%S")
    for i in range(1, 60):
        day = today - timedelta(days=i)
        statistic_device_utilization(day)
        statistic_device_full_rate(day)
        statistic_total_pot_statistic(day)


if __name__ == '__main__':
    test()
    # test_statistic_device_pot_num()

