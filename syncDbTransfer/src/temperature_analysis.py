# coding:utf-8

import datetime
import re
import pymongo
import pprint
import math
import numpy as np
from collections import OrderedDict

from matplotlib import pyplot
import matplotlib.pyplot as plt

from tools import DBManager

LATEST_NUM = 5

TEMPERATURE_FLUCTUATE_RATE = 0.9
COOL_TOP_TEMPERATURE = 1.2
TOP_TEMPERATURE = 1.1
BOTTOM_TEMPERATURE = 0.9

TOP_KEEP_TIME = 1.1
BOTTOM_KEEP_TIME = 0.9
KEEP_BLOCK_RATE = 0.5
COOL_BLOCK_RATE = 0.5

# 波动边界方差
KEEP_STD_MARGIN = 20
CHANGE_STD_MARGIN = 30
# 保温范围
KEEP_RANGE = 50
# 保温最短持续时间
KEEP_MIN_LAST = 10
RESULT = [
    '正常',
    '堵缸',
    '升温太快',
    '升温太慢',
    '温控波动太大',
    '恒温过长',
    '恒温过短',
    '降温太快',
    '降温太慢',
    '恒温波动太大'
]
# 正常 0
# 堵缸 1
# 升温太快 2
# 升温太慢 3
# 温控波动太大 4
# 恒温过长 5
# 恒温过短 6
# 降温太快 7
# 降温太慢 8
# 恒温波动太大 9


class TemperatureAnalysis(object):

    def analysis(self, real_pro, plan_pro):
        self.real_pro = real_pro
        self.plan_pro = plan_pro

        t_rise = real_pro[0]
        p_rise = plan_pro[0]
        syn_len = min(len(t_rise), len(p_rise))
        rise_result = 0
        if syn_len:
            rise_result = self.get_rise_analysis(t_rise[:syn_len], p_rise[:syn_len])

        t_keep = real_pro[1]
        p_keep = plan_pro[1]
        keep_result = self.get_keep_analysis(t_keep, p_keep)
        if not t_keep and not real_pro[2]:
            # 不存在恒温与降温数据，升温异常为堵缸
            if rise_result:
                keep_result = 1

        cool_result = 0
        t_cool = real_pro[2]
        p_cool = plan_pro[2]
        syn_len = min(len(t_cool), len(p_cool))
        if syn_len:
            cool_result = self.get_cooling_analysis(t_cool[:syn_len], p_cool[:syn_len])

        results = []
        for r in (rise_result, keep_result, cool_result):
            if isinstance(r, tuple):
                for r_int in r:
                    results.append(str(r_int))
            else:
                if r:
                    results.append(str(r))
        if not results:
            results = '0'
        else:
            results = sorted(list(set(results)))
            results = ''.join(results)
        return results

    def get_rise_analysis(self, real_list, plan_list):
        # 曲率为负
        if real_list[-1] < real_list[0]:
            return 1

        diffs = [x - y for x, y in zip(real_list, plan_list)]
        diff_std = np.std(diffs)

        real_plan_ratio = sum(real_list) / sum(plan_list)

        tmpt_gt_rate = self.get_gt_rate(real_list, plan_list)
        latest_gt = self.is_latest_gt(
            real_list[-LATEST_NUM:], plan_list[-LATEST_NUM:], TOP_TEMPERATURE)
        # 实际温度多数时间高于设定值
        if tmpt_gt_rate > TEMPERATURE_FLUCTUATE_RATE:
            if real_plan_ratio > TOP_TEMPERATURE:
                if latest_gt:
                    return 2
                else:
                    return 0
            else:
                if diff_std < CHANGE_STD_MARGIN:
                    return 0

        latest_lt = self.is_latest_lt(
            real_list[-LATEST_NUM:], plan_list[-LATEST_NUM:], BOTTOM_TEMPERATURE)
        tmpt_lt_rate = self.get_lt_rate(real_list, plan_list)
        # 实际温度多数时间低于设定值
        if tmpt_lt_rate > TEMPERATURE_FLUCTUATE_RATE:
            if real_plan_ratio < BOTTOM_TEMPERATURE:
                if latest_lt:
                    return 3
            else:
                if diff_std < CHANGE_STD_MARGIN:
                    return 0

        # 温度时高时低，标准差判断
        diffs = [x - y for x, y in zip(real_list, plan_list)]
        diff_std = np.std(diffs)
        if diff_std < CHANGE_STD_MARGIN:
            # 正常波动
            return 0
        else:
            # 波动太大+升温异常
            if latest_gt:
                return 2, 4
            elif latest_lt:
                return 3, 4
            else:
                return 4

    def get_keep_analysis(self, real_list, plan_list):
        if len(plan_list) == 0:
            # 不存在计划恒温过程
            return 0
        if len(real_list) == 0 and not self.real_pro[2]:
            # 数据不足只有升温过程
            return 0
        if len(real_list) == 0 and len(plan_list) > 0:
            # 实际没有恒温过程
            return 1

        keep_time = self.get_keep_time(real_list, plan_list[0])
        plan_keep_rate = keep_time / len(plan_list)
        if plan_keep_rate > TOP_KEEP_TIME:
            return 5
        # 数据完整，存在降温数据
        if self.real_pro[2]:
            if plan_keep_rate < BOTTOM_KEEP_TIME:
                if plan_keep_rate < KEEP_BLOCK_RATE:
                    # 堵缸
                    return 1
                # 恒温过短
                return 6
            else:
                if np.std(real_list) > KEEP_STD_MARGIN:
                    # 恒温波动太大
                    return 9
                else:
                    return 0
        else:
            real_keep_rate = keep_time / len(real_list)
            if real_keep_rate < BOTTOM_KEEP_TIME:

                if real_keep_rate < KEEP_BLOCK_RATE:
                    # 堵缸
                    return 1
                else:
                    # 恒温过短
                    return 6
            else:
                if np.std(real_list) > KEEP_STD_MARGIN:
                    # 恒温波动太大
                    return 9
                else:
                    return 0

    def get_keep_time(self, real_list, keep_value):
        # 恒温范围setting_temperature ± KEEP_RANGE
        # 不处于恒温状态标志：前三分或后三分都不在恒温范围内（包含自身）
        range_ = (keep_value-KEEP_RANGE, keep_value+KEEP_RANGE)
        out_range_count = 0
        for i in range(len(real_list)):
            v_pre = real_list[i-2:i+1]
            v_suf = real_list[i:i+3]
            if self.is_out_range(v_pre, range_) or self.is_out_range(v_suf, range_):
                out_range_count += 1
        in_range_count = len(real_list) - out_range_count
        return in_range_count

    def is_out_range(self, values, range_):
        if len(values) < 3:
            return False
        for v in values:
            if range_[0] <= v <= range_[1]:
                return False
        return True

    def is_latest_gt(self, real_list, plan_list, rate=1.0):
        if len(real_list) < LATEST_NUM:
            return False
        for t1, t2 in zip(real_list, plan_list):
            if t1 < t2 * rate:
                return False
        return True

    def is_latest_lt(self, real_list, plan_list, rate=1.0):
        if len(real_list) < LATEST_NUM:
            return False
        for t1, t2 in zip(real_list, plan_list):
            if t1 > t2 * rate:
                return False
        return True

    def get_cooling_analysis(self, real_list, plan_list):
        # 曲率为正
        if real_list[-1] > real_list[0]:
            return 1

        real_plan_ratio = sum(real_list) / sum(plan_list)

        tmpt_gt_rate = self.get_gt_rate(real_list, plan_list)
        latest_gt = self.is_latest_gt(
            real_list[-LATEST_NUM:], plan_list[-LATEST_NUM:], COOL_TOP_TEMPERATURE)
        # 实际温度多数时间高于设定值
        if tmpt_gt_rate > TEMPERATURE_FLUCTUATE_RATE:
            if real_plan_ratio > COOL_TOP_TEMPERATURE:
                if latest_gt:
                    return 8
                else:
                    return 0
            else:
                return 0

        latest_lt = self.is_latest_lt(
            real_list[-LATEST_NUM:], plan_list[-LATEST_NUM:], BOTTOM_TEMPERATURE)
        tmpt_lt_rate = self.get_lt_rate(real_list, plan_list)
        # 实际温度多数时间低于设定值
        if tmpt_lt_rate > TEMPERATURE_FLUCTUATE_RATE:
            if real_plan_ratio < BOTTOM_TEMPERATURE:
                if real_list[-1] / plan_list[-1] > COOL_BLOCK_RATE:
                    if self.is_latest_gt(real_list[-LATEST_NUM:], plan_list[-LATEST_NUM:],
                                         COOL_BLOCK_RATE):
                        return 0
                    else:
                        return 7
                else:
                    return 1
            else:
                return 0

        # 温度时高时低，标准差判断
        diffs = [x - y for x, y in zip(real_list, plan_list)]
        diff_std = np.std(diffs)
        if diff_std < CHANGE_STD_MARGIN * 1.5:
            # 正常波动
            return 0
        else:
            # 波动太大+降温异常
            if latest_gt:
                return 8, 4
            elif latest_lt:
                return 7, 4
            else:
                return 4

    def get_gt_rate(self, real_list, plan_list):
        n = len(real_list)
        gt_count = 0
        for t1, t2 in zip(real_list, plan_list):
            if t1 > t2:
                gt_count += 1
        return gt_count / n

    def get_lt_rate(self, real_list, plan_list):
        n = len(real_list)
        gt_count = 0
        for t1, t2 in zip(real_list, plan_list):
            if t1 < t2:
                gt_count += 1
        return gt_count / n

    def read_cursor(self, cursor):
        try:
            return cursor.next()
        except StopIteration:
            return None


def pro_divide(start, end, tmpts, tags):
    if end - start + 1 >= KEEP_MIN_LAST:
        pros = (
            tmpts[:start],
            tmpts[start:end+1],
            tmpts[end+1:]
        )
    else:
        if -1 in tags:
            cool_index = tags.index(-1)
            pros = (
                tmpts[:cool_index],
                [],
                tmpts[cool_index:]
            )
        else:
            pros = (tmpts, [], [])
    return pros


def get_all_data(cursor_t, cursor_p):
    ta = TemperatureAnalysis()

    tmpt_dict = {}
    for t in cursor_t:
        scheduling_id = t['scheduling_id']
        if scheduling_id not in tmpt_dict:
            tmpt_dict[scheduling_id] = []
        sch_list = tmpt_dict[scheduling_id]
        sch_list.append(t)

    plan_dict = {}
    for p in cursor_p:
        scheduling_id = p['scheduling_id']
        if scheduling_id not in plan_dict:
            plan_dict[scheduling_id] = []
        sch_list = plan_dict[scheduling_id]
        sch_list.append(p)

    all_data = []
    for s_id in tmpt_dict:
        # t_cursor = table_t.find({'scheduling_id': s_id}).sort('timestamp')
        # p_cursor = table_p.find({'scheduling_id': s_id}).sort('timestamp')
        # if not(t_cursor.count() and p_cursor.count()):
        #     continue
        # start = min(t_cursor[0]['timestamp'], p_cursor[0]['timestamp'])
        if s_id not in plan_dict:
            continue
        t_cursor = sorted(tmpt_dict[s_id], key=lambda x: x['timestamp'])
        p_cursor = sorted(plan_dict[s_id], key=lambda x: x['timestamp'])
        if len(t_cursor) == 0 or len(p_cursor) == 0:
            continue
        # print(t_cursor)
        t_start = t_cursor[0]['timestamp']
        p_start = p_cursor[0]['timestamp']
        # 时间统一化处理
        start_point = max(t_start, p_start)
        while t_cursor[0]['timestamp'] < start_point:
            t_cursor = t_cursor[1:]
            if len(t_cursor) == 0:
                break
        while p_cursor[0]['timestamp'] < start_point:
            p_cursor = p_cursor[1:]
            if len(p_cursor) == 0:
                break
        if len(t_cursor) == 0 or len(p_cursor) == 0:
            continue
        time = t_cursor[0]['timestamp']
        day = datetime.datetime(time.year, time.month, time.day)
        device_id = t_cursor[0]['device_id']
        timestamp = t_cursor[-1]['timestamp']
        keep_start = 0
        keep_end = 0
        keep_count = 0
        index = 0
        t_list = []
        # -1降温，0保温，1升温
        t_tags = []
        stop_index = []  # 剔除暂停的时间
        init_list = []
        # initx = []
        for t in t_cursor:

            cur_t = t['current_temperature']
            # initx.append((t['timestamp'] - start).total_seconds() / 60)
            init_list.append(cur_t)
            function_name = t['function_name']

            if function_name[:2] == '温控':
                plan_t = re.findall(r"\d+\.?\d*", function_name)
                plan_t = int(plan_t[-1]) * 10
                # 剔除暂停状态的数据，识别方法：
                # 温控状态与前一分钟和上一分钟温度变化都小于0.5度
                if len(init_list) > 2 and plan_t > cur_t:
                    if abs(cur_t - init_list[-2]) < 5 and abs(cur_t - init_list[-3]) < 5:
                        stop_index.append(index)
                if keep_count:
                    keep_start = index - keep_count
                    keep_end = index - 1
                    keep_count = 0

                if plan_t > cur_t:
                    t_tags.append(1)
                else:
                    t_tags.append(-1)
            else:
                keep_count += 1
                t_tags.append(0)
            t_list.append(cur_t)
            index += 1

            # x1.append((t['timestamp'] - start).total_seconds() / 60 - shift_time)
            # y1.append(cur_t)
        # 无降温过程
        if keep_count:
            keep_start = index - keep_count
            keep_end = index - 1
        # 修正实际暂停时间

        while len(stop_index) > 5:
            if stop_index[1] - stop_index[0] > 3:
                stop_index = stop_index[1:]
            elif stop_index[2] - stop_index[0] > 6:
                stop_index = stop_index[1:]
            elif stop_index[3] - stop_index[0] > 10:
                stop_index = stop_index[1:]
            else:
                break

        while len(stop_index) > 5:
            if stop_index[-1] - stop_index[-2] > 3:
                stop_index = stop_index[:-1]
            elif stop_index[-1] - stop_index[-3] > 6:
                stop_index = stop_index[:-1]
            elif stop_index[-1] - stop_index[-4] > 10:
                stop_index = stop_index[:-1]
            else:
                break

        if len(stop_index) > 5:
            stop_start = stop_index[0]
            stop_end = stop_index[-1]
            stop_gap = stop_end - stop_start + 1
            if stop_gap < len(stop_index) * 2 and \
                    t_list[stop_end] < t_list[stop_start] + stop_gap * 4:

                t_list = t_list[0:stop_start+1] + t_list[stop_end+1:]
                if keep_start > stop_start:
                    # 恒温起始下标修正
                    keep_start -= stop_gap
                    keep_end -= stop_gap

        p_list = []
        p_keep_start = 0
        p_keep_end = 0
        p_keep_count = 0
        index = 0
        p_tags = [1]
        y2 = []
        for p in p_cursor:
            # x2.append((p['timestamp'] - start).total_seconds() / 60)
            cur_t = p['setting_temperature']
            y2.append(cur_t)
            st = p['setting_temperature']
            if p_list:
                if st == p_list[-1]:
                    p_keep_count += 1
                    p_tags.append(0)
                else:
                    if p_keep_count:
                        p_keep_start = index - p_keep_count
                        p_keep_end = index - 1
                        p_keep_count = 0

                    if st > p_list[-1]:
                        p_tags.append(1)
                    else:
                        p_tags.append(-1)
            p_list.append(st)
            index += 1
        # 无降温过程
        if p_keep_count:
            p_keep_start = index - p_keep_count
            p_keep_end = index - 1

        # 过程划分
        real_pros = pro_divide(keep_start, keep_end, t_list, t_tags)
        plan_pros = pro_divide(p_keep_start, p_keep_end, p_list, p_tags)

        analysis_result = ta.analysis(real_pros, plan_pros)
        data_dict = {
            'day': day,
            'device_id': device_id,
            'analysis_result': analysis_result,
            'scheduling_id': s_id,
            'timestamp': timestamp

        }
        all_data.append(data_dict)
    return all_data


def write_latest_temperature_analysis():
    table_t = DBManager('report', 'device_latest_temperature')
    table_p = DBManager('report', 'device_latest_plan_temperature')
    all_data = get_all_data(table_t.find(), table_p.find())

    table_analysis = DBManager('report', 'latest_temperature_analysis')
    for data_dict in all_data:
        table_analysis.update_data({'scheduling_id': data_dict['scheduling_id']},
                                   data_dict)
    if table_analysis.find().count() > 1000:
        table_analysis.delete_days(2)


def write_temperature_analysis():
    start_date = datetime.datetime(2019, 5, 1)
    query_filter = {'timestamp': {'$gte': start_date}}
    table_t = DBManager('report', 'device_temperature')
    table_p = DBManager('report', 'device_plan_temperature')
    cursor_t = table_t.find(query_filter)
    cursor_p = table_p.find(query_filter)
    all_data = get_all_data(cursor_t, cursor_p)
    table_analysis = DBManager('report', 'temperature_analysis')
    table_analysis.insert_data(all_data)


if __name__ == '__main__':
    write_latest_temperature_analysis()