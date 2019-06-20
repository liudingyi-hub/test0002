import logging
import traceback

import pymongo
from datetime import datetime, timedelta
from tools import get_process_id, DBManager, get_device_id

latest_temperature = 0
latest_plan = 0


# 设备实时状态更新
def write_device_status(t_devicestatus, pre_id='01001'):
    logging.getLogger('transfer').info('write_device_status.')
    db = DBManager('report', 'device_status')
    for item in t_devicestatus:
        start_time = None
        try:
            start_time = datetime.strptime(item[18], '%Y-%m-%d %X')
        except Exception as e:
            logging.getLogger('transfer').warning(traceback.format_exc())
            logging.getLogger('transfer').warning('%s' % e)
        data = {
            'device_id': get_device_id(item[0], pre_id),
            'scheduling_id': item[2],
            'technics_id': item[3],
            'step_id': item[4],
            'process_id': get_process_id(item[8]),
            'status': item[1],
            'init_temperature': item[17],
            'start_time': start_time,
        }
        try:
            db.update_data(['device_id'], data)
        except Exception as e:
            logging.getLogger('transfer').error('update device_status failed.')
            logging.getLogger('transfer').error(traceback.format_exc())
            logging.getLogger('transfer').error(e)


# 设备实时计划步骤更新
def write_device_step(t_devicestep, pre_id='01001'):
    logging.getLogger('transfer').info('write_device_step.')
    db = DBManager('report', 'device_step')
    db.remove_all()
    data_all = []
    for item in t_devicestep:
        data = {
            'device_id': get_device_id(item[1], pre_id),
            'technics_id': item[2],
            'step_id': item[3],
            'function_id': item[4],
            'param1': item[5],
            'param2': item[6],
            'param3': item[7],
            'note': item[8],
        }
        data_all.append(data)
    db.insert_data(data_all)


# 设备温度表
def write_device_temperature(t_temperature, pre_id='01001'):
    logging.getLogger('transfer').info('write_device_temperature.')
    db = DBManager('report', 'device_temperature')
    db_latest = DBManager('report', 'device_latest_temperature')
    all_data = []
    for item in t_temperature:
        time_ = None
        try:
            time_ = datetime.strptime(item[8], '%Y-%m-%d %X')
        except Exception as e:
            logging.getLogger('transfer').warning(traceback.format_exc())
            logging.getLogger('transfer').warning(e)
        logging.getLogger('transfer').debug(time_)
        single_data = {
            'device_id': get_device_id(item[1], pre_id),
            'scheduling_id': item[7],
            'operator': '',
            'step_id': item[4],
            'function_name': item[5],
            'current_temperature': item[2],
            'status': item[6],
            'device_status': item[9],
            'timestamp': time_,
        }
        # logging.getLogger('transfer').debug(single_data)
        all_data.append(single_data)
    # logging.getLogger('transfer').debug(all_data)
    db.insert_data(all_data)
    db_latest.insert_data(all_data)
    global latest_temperature
    if latest_temperature >= 50:
        db_latest.delete_days(2)
        latest_temperature = 0
    else:
        latest_temperature += 1


# 设备历史工艺表
def write_device_history_step(t_devicehistorystep, pre_id='01001'):
    logging.getLogger('transfer').info('write_device_history_step.')
    db = DBManager('report', 'device_history_step')
    all_data = []
    all_step = {}
    for item in t_devicehistorystep:
        scheduling_id = item[2]
        single_data = {
            'device_id': get_device_id(item[1], pre_id),
            'scheduling_id': scheduling_id,
            'technics_id': item[3],
            'step_id': item[4],
            'function_id': item[5],
            'param1': item[6],
            'param2': item[7],
            'param3': item[8],
            'note': item[9],
        }
        all_data.append(single_data)
        if scheduling_id not in all_step:
            all_step[scheduling_id] = []
        all_step[scheduling_id].append(single_data)
    db.insert_data(all_data)


# 设备历史时间表
def write_device_history_info(t_devicehistorystatus, pre_id='01001'):
    logging.getLogger('transfer').info('write_device_history_info.')
    db = DBManager('report', 'device_history_info')
    all_data = []
    for item in t_devicehistorystatus:
        process_id = get_process_id(item[8])
        start_time = None
        end_time = None
        try:
            start_time = datetime.strptime(item[-2], '%Y-%m-%d %X')
            end_time = datetime.strptime(item[-1], '%Y-%m-%d %X')
        except Exception as e:
            logging.getLogger('transfer').warning(traceback.format_exc())
            logging.getLogger('transfer').warning(e)
        single_data = {
            'device_id': get_device_id(item[1], pre_id),
            'scheduling_id': item[2],
            'process_id': process_id,
            'technics_id': item[3],
            'step_id': item[4],
            'init_temperature': item[-3],
            'start_time': start_time,
            'end_time': end_time,
            'is_flaw': 0,
            'is_block_off': 0,
            'is_success_once': 1,
        }
        all_data.append(single_data)
    db.insert_data(all_data)


# 写入计划温度曲线
def write_plan_data(scheduling_id, technics_id, temp_step, init_temperature, start_time):
    # logging.getLogger('transfer').info('write_plan_data.')
    # db = DBManager(db_name, 'device_plan_temperature')
    t = init_temperature
    time_ = datetime.strptime(start_time.strftime("%Y-%m-%d %H:%M:")+'00', '%Y-%m-%d %X')
    data = []
    data.append({'scheduling_id': scheduling_id, 'technics_id': technics_id,
                 'step_id': temp_step[0][0], 'setting_temperature': t, 'timestamp': time_})
    for item in temp_step:
        init_temperature = t
        if item[1] == 1:
            k = item[2]*10 + item[3]
            if item[4]*10 < init_temperature:
                k *= -1
            while (k > 0 and t < item[4]*10) or (k < 0 and t > item[4]*10):
                t += k
                time_ += timedelta(minutes=1)
                data.append({'scheduling_id': scheduling_id, 'technics_id': technics_id,
                             'step_id': item[0], 'setting_temperature': t, 'timestamp': time_})
        elif item[1] == 2:
            count = item[2]
            for i in range(count):
                time_ += timedelta(minutes=1)
                data.append({'scheduling_id': scheduling_id, 'technics_id': technics_id,
                             'step_id': item[0], 'setting_temperature': t, 'timestamp': time_})
        elif item[1] == 3:
            count = item[2]
            for i in range(count):
                time_ += timedelta(seconds=1)
                time_ = datetime.strptime(time_.strftime("%Y-%m-%d %H:%M:")+'00', '%Y-%m-%d %X')
                if data and time_ == data[-1]['timestamp']:
                    data.pop()
                data.append({'scheduling_id': scheduling_id, 'technics_id': technics_id,
                             'step_id': item[0], 'setting_temperature': t, 'timestamp': time_})
        elif item[1] == 0:
            break
    return data


device_scheduling_id = {}


# 获取当前设备的设定值，然后写入设定温度曲线
def write_device_plan_temperature(pre_id='01001', **kwargs):
    logging.getLogger('transfer').info('write_device_plan_temperature.')
    step_db = DBManager('report', 'device_step')
    status_db = DBManager('report', 'device_status')

    latest_plan_db = DBManager('report', 'device_latest_plan_temperature')
    latest_plans = latest_plan_db.distinct('scheduling_id')
    all_latest_plans = set(latest_plans)

    all_data = status_db.find({})
    if not all_data.count():
        logging.getLogger('transfer').error('get data from device_status error.')
        return
    global device_scheduling_id
    write_data = []
    for single in all_data:
        # device_id = get_device_id(single['device_id'], pre_id)
        device_id = single['device_id']
        scheduling_id = single['scheduling_id']
        if device_id not in device_scheduling_id:
            device_scheduling_id[device_id] = scheduling_id
        else:
            if scheduling_id == device_scheduling_id[device_id]:
                continue
        if scheduling_id in all_latest_plans:
            continue
        technics_id = single['technics_id']
        status_step = single['step_id']
        init_temperature = single['init_temperature']
        start_time = single['start_time']
        if not start_time or not init_temperature or not scheduling_id or not device_id \
                or not technics_id or not status_step:
            logging.getLogger('transfer').error('data in device_status error.scheduling_id: %s' % scheduling_id)
            continue

        step_data = step_db.find_sort({'device_id': device_id, 'technics_id': technics_id},
                                      [('step_id', pymongo.ASCENDING)])
        if not step_data.count():
            continue
        temp_step = []
        for step in step_data:
            temp = (step['step_id'], step['function_id'], step['param1'], step['param2'], step['param3'])
            temp_step.append(temp)
        if temp_step and temp_step[0][0] == status_step:
            data_temp = write_plan_data(scheduling_id, technics_id, temp_step, init_temperature, start_time)
            if data_temp:
                write_data += data_temp
            device_scheduling_id[device_id] = scheduling_id
    latest_plan_db.insert_data(write_data)
    global latest_plan
    if latest_plan >= 50:
        latest_plan_db.delete_days(2)
        latest_plan = 0
    else:
        latest_plan += 1


# 获取当前设备的设定值，然后写入设定温度曲线
# date_time: '20190101'
def write_device_history_plan_temperature(date_time='20190529', pre_id='01001', **kwargs):
    logging.getLogger('transfer').info('write_device_history_plan_temperature.')
    step_db = DBManager('report', 'device_history_step')
    status_db = DBManager('report', 'device_history_info')
    plan_db = DBManager('report', 'device_plan_temperature')
    latest_plan_db = DBManager('report', 'device_latest_plan_temperature')

    filter_ = {'scheduling_id': {'$gte': date_time+'%'}}
    all_data = status_db.find(filter_)
    if not all_data.count():
        logging.getLogger('transfer').error('get data from device_history_info error.')
        return
    step_data = step_db.find_sort(filter_, [('step_id', pymongo.ASCENDING)])
    if not all_data.count():
        logging.getLogger('transfer').error('get data from device_history_step error.')
        return
    all_step = {}
    for item in step_data:
        if item['scheduling_id'] not in all_step:
            all_step[item['scheduling_id']] = []
        all_step[item['scheduling_id']].append(item)

    plans = plan_db.distinct('scheduling_id')
    all_plan = set(plans)
    latest_plans = latest_plan_db.distinct('scheduling_id')
    all_latest_plans = set(latest_plans)

    write_data = []
    latest_data = []

    for single in all_data:
        # device_id = get_device_id(single['device_id'], pre_id)
        device_id = single['device_id']
        scheduling_id = single['scheduling_id']
        technics_id = single['technics_id']
        status_step = single['step_id']
        init_temperature = single['init_temperature']
        start_time = single['start_time']
        if not start_time or not init_temperature or not scheduling_id or not device_id \
                or not technics_id or not status_step:
            logging.getLogger('transfer').warning('data in device_history_info error.scheduling_id: %s' % scheduling_id)
            continue
        temp_step = []
        if scheduling_id not in all_step or (scheduling_id in all_plan and scheduling_id in all_latest_plans):
            continue
        step_data = all_step[scheduling_id]
        for step in step_data:
            temp = (step['step_id'], step['function_id'], step['param1'], step['param2'], step['param3'])
            temp_step.append(temp)
        if temp_step and temp_step[0][0] == status_step:
            data_temp = write_plan_data(scheduling_id, technics_id, temp_step, init_temperature, start_time)
            if data_temp:
                if scheduling_id not in all_plan:
                    write_data += data_temp
                if start_time > datetime.now()-timedelta(days=2) and scheduling_id not in all_latest_plans:
                    latest_data += data_temp
    logging.getLogger('transfer').info('start to write plan_temperature. %d' % len(write_data))
    plan_db.insert_data(write_data)

    logging.getLogger('transfer').info('start to write latest_plan_temperature. %d' % len(latest_data))
    latest_plan_db.insert_data(latest_data)


def get_data_by_date(date_time):
    all_ = {}
    status_db = DBManager('report', 'device_history_info')
    filter_ = {'start_time': {'$gte': datetime.strptime(date_time, '%Y%m%d')}}
    all_data = status_db.find(filter_)
    if not all_data.count():
        logging.getLogger('transfer').error('get data from device_history_info error.')
        return
    for item in all_data:
        time_str = item['start_time'].strftime("%Y%m%d")
        if time_str not in all_:
            all_[time_str] = {}
        if item['device_id'] not in all_[time_str]:
            all_[time_str][item['device_id']] = []
        all_[time_str][item['device_id']].append(item)
    return all_


def get_all_plan_data(date_time):
    all_ = {}
    plan_db = DBManager('report', 'device_plan_temperature')
    filter_ = {'scheduling_id': {'$gte': date_time+'%'}}
    all_data = plan_db.find(filter_)
    if not all_data.count():
        logging.getLogger('transfer').error('get data from device_history_info error.')
        return
    for item in all_data:
        if not item['scheduling_id'] or len(str(item['scheduling_id'])) < 12:
            continue
        time_str = str(item['scheduling_id'])[:8]
        if time_str not in all_:
            all_[time_str] = {}
        if item['scheduling_id'] not in all_[time_str]:
            all_[time_str][item['scheduling_id']] = []
        all_[time_str][item['scheduling_id']].append(item)
    return all_


def get_plan_time(plans):
    if not plans:
        return
    min_ = datetime.now()
    max_ = datetime.strptime('2015-01-01', '%Y-%m-%d')
    get_min = False
    get_max = False
    for item in plans:
        if plans['timestamp'] < min_:
            min_ = plans['timestamp']
            get_min = True
        if plans['timestamp'] > max_:
            max_ = plans['timestamp']
            get_max = True
    if get_max and get_min:
        return (max_ - min_).seconds
    else:
        return None


def get_device_capacity():
    all_ = {}
    db = DBManager('report', 'device_base_info')
    all_data = db.find({})
    if not all_data.count():
        logging.getLogger('transfer').error('get data from device_base_info error.')
        return
    for item in all_data:
        all_[item['id']] = item['capacity']
    return all_


def get_work_time_rate(all_data):
    result = {}
    waite = {}
    total_pot = {}
    change_time = 60*5
    for time_ in all_data:
        result[time_] = {}
        waite[time_] = {}
        total_pot[time_] = {}
        for device in all_data[time_]:
            result[time_][device] = {}
            waite[time_][device] = {}
            pots = all_data[time_][device]
            count = len(pots)
            total_pot[time_][device] = count
            if count == 0:
                result[time_][device]['work_time_rate'] = 0
                waite[time_][device]['waite_time'] = 24.0
                continue
            work_time = timedelta(0)
            for item in pots:
                if item['end_time'] > datetime.strptime(time_, '%Y%m%d') + timedelta(days=1):
                    work_time += datetime.strptime(time_, '%Y%m%d') + timedelta(days=1) - item['start_time']
                else:
                    work_time += item['end_time'] - item['start_time']
            work_time_int = int(work_time.seconds)
            result[time_][device]['work_time_rate'] = 1.0 * (work_time_int + change_time * count) / (24 * 3600)
            waite[time_][device]['waite_time'] = 24.0 - 1.0 * (work_time_int + change_time * count) / 3600
    return result, waite, total_pot


def get_work_time_error_pot(all_data, plan_data):
    result = {}
    for time_ in all_data:
        result[time_] = {}
        if time_ not in plan_data:
            continue
        for device in all_data[time_]:
            result[time_][device] = {}
            pots = all_data[time_][device]
            count = len(pots)
            if count == 0:
                result[time_][device]['work_time_error_pot'] = 0
                continue
            for item in pots:
                if not item['scheduling_id'] or item['scheduling_id'] not in plan_data[time_]:
                    continue
                plan_time = get_plan_time(plan_data[time_][item['scheduling_id']])
                if not plan_time:
                    continue
                work_time = (item['end_time'] - item['start_time']).seconds
                if 1.0*(work_time-plan_time)/plan_time > 0.2:
                    result[time_][device]['work_time_error_pot'] += 1
    return result


# def get_weight():
#     result = {}
#     for time_ in all_data:
#         result[time_] = {}
#         if time_ not in plan_data:
#             continue
#         for device in all_data[time_]:
#             result[time_][device] = {}
#             pots = all_data[time_][device]
#             count = len(pots)
#             if count == 0:
#                 result[time_][device]['work_time_error_pot'] = 0
#                 continue
#             for item in pots:


def write_device_static(date_time='20190529'):
    all_data = get_data_by_date(date_time)
    plan_data = get_all_plan_data(date_time)
    device_capacity = get_device_capacity()
    work_time_rate, waite_time, total_pot = get_work_time_rate(all_data)
    work_time_error_pot = get_work_time_error_pot(all_data, plan_data)
    # full_rate = get_full_rate(all_data, device_capacity)
    pass

