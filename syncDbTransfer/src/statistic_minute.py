import pymongo
import time
import logging
from datetime import datetime
from datetime import timedelta

from tools import DBManager


def statistic_device_standby_time(today=None):
    if today is None:
        today = datetime.strptime(time.strftime("%Y-%m-%d 00:00:00"), "%Y-%m-%d %H:%M:%S")
    tomorrow = today + timedelta(days=1)
    current_db = DBManager('report', 'device_status')
    his_db = DBManager('report', 'device_history_info')
    db = DBManager('report', 'device_standby_statistic')
    current_data = current_db.find()
    his_data = his_db.find({'end_time': {'$gte': today, '$lt': tomorrow}})
    now = min(datetime.now(), tomorrow)
    all_seconds = int((now - today).total_seconds())
    device_time = {}
    for element in his_data:
        device_id = element.get('device_id', 0)
        scheduling_id = element.get('scheduling_id', None)
        start_time = element.get('start_time', None)
        end_time = element.get('end_time', None)
        if not device_id or not scheduling_id or not start_time or not end_time:
            continue
        if start_time < today:
            start_time = today
        work_time = (end_time - start_time).total_seconds()
        if work_time <= 0:
            continue
        if device_id not in device_time:
            device_time[device_id] = {scheduling_id: work_time}
        else:
            device_time[device_id][scheduling_id] = work_time
    for element in current_data:
        device_id = element.get('device_id', 0)
        scheduling_id = element.get('scheduling_id', None)
        start_time = element.get('start_time', None)
        end_time = element.get('end_time', now)
        status = element.get('status')
        if device_id not in device_time:
            device_time[device_id] = {}
        if not scheduling_id or scheduling_id in device_time[device_id]:
            continue
        if (status == 2 or status == 3) and start_time:
            if start_time < today:
                start_time = today
            work_time = (end_time - start_time).total_seconds()
            if work_time <= 0:
                continue
            # print(device_id, element.get('status'), start_time, end_time)
            device_time[device_id][scheduling_id] = work_time
        # else:
        #     print(device_id, element.get('status'), start_time, end_time)
    for device_id, scheduling_ts in device_time.items():
        work_time = sum(scheduling_ts.values())
        pot_num = len(scheduling_ts.values())
        standby_time = all_seconds - work_time
        if standby_time < 0:
            standby_time = 0
            logging.getLogger('transfer').warning('device work time[%s] larger than seconds[%s]' % (work_time, all_seconds))
        data = {
            'day': today,
            'device_id': device_id,
            'pot_num': pot_num,
            'standby_time': standby_time
        }
        db.update_data(['day', 'device_id'], data)


def statistic_device_work_pot(today=None):
    global PLOT_WORK_TIME_ERROR_PERCENT
    PLOT_WORK_TIME_ERROR_PERCENT = 20
    if today is None:
        today = datetime.strptime(time.strftime("%Y-%m-%d 00:00:00"), "%Y-%m-%d %H:%M:%S")
    status_db = DBManager('report', 'device_history_info')
    step_db = DBManager('report', 'device_history_step')
    db = DBManager('report', 'device_work_pot')
    date_time = today.strftime("%Y%m%d")
    filter_ = {'scheduling_id': {"$regex": date_time}}
    all_data = status_db.find(filter_)
    step_data = step_db.find_sort(filter_, [('step_id', pymongo.ASCENDING)])
    all_step = {}
    device_plot_data = {}
    for item in step_data:
        if item['scheduling_id'] not in all_step:
            all_step[item['scheduling_id']] = []
        all_step[item['scheduling_id']].append(item)
    for single in all_data:
        device_id = single['device_id']
        scheduling_id = single['scheduling_id']
        technics_id = single['technics_id']
        status_step = single['step_id']
        init_temperature = single['init_temperature']
        start_time = single['start_time']
        end_time = single['end_time']
        if device_id in device_plot_data:
            device_plot_data[device_id]["num"] += 1
        else:
            device_plot_data[device_id] = {"device_id": device_id, "day": today, "num": 1, "delay_num": 0}
        if not start_time or not end_time or not init_temperature or not scheduling_id or not device_id \
                or not technics_id or not status_step:
            continue
        if scheduling_id not in all_step:
            continue
        step_data = all_step[scheduling_id]
        temp_step = []
        for step in step_data:
            temp = (step['step_id'], step['function_id'], step['param1'], step['param2'], step['param3'])
            temp_step.append(temp)
        if temp_step and temp_step[0][0] != status_step:
            continue
        plan_seconds = 0
        for item in temp_step:
            if item[1] == 1:
                k = item[2] * 10 + item[3]
                plan_seconds += abs(int((init_temperature - item[4] * 10.0) / k * 60))
            elif item[1] == 2:
                plan_seconds += item[2] * 60
            elif item[1] == 3:
                plan_seconds += item[2]
        work_seconds = int((end_time - start_time).total_seconds())
        if (work_seconds - plan_seconds) * 100 / plan_seconds > PLOT_WORK_TIME_ERROR_PERCENT:
            device_plot_data[device_id]["delay_num"] += 1
    for data in device_plot_data.values():
        db.update_data(["device_id", "day"], data)


def test():
    today = datetime.strptime(time.strftime("%Y-%m-%d 00:00:00"), "%Y-%m-%d %H:%M:%S")
    for i in range(0, 60):
        day = today - timedelta(days=i)
        statistic_device_standby_time(today=day)
        statistic_device_work_pot(today=day)


if __name__ == '__main__':
    test()
    # test()
    # d1 = datetime(2019, 6, 5)
    # d2 = datetime(2019, 6, 4, 0)
    # print((d1-d2).total_seconds())


