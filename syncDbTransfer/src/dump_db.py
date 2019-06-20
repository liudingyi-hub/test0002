from tools import DBManager
import json
from datetime import datetime, timedelta
from copy import deepcopy

def dump_device_base_info():

    db = DBManager('report', 'device_base_info')
    all_data = db.find()
    for data in all_data:
        new_data = {}
        for k, v in data.items():
            if k in ('_id', 'insert_time'):
                continue
            new_data[k] = v
        print(new_data)


def dump_card():
    db = DBManager('report', 'card')
    all_data = db.find()
    dump_data = []
    for data in all_data:
        new_data = {}
        for k, v in data.items():
            if k in ('_id', 'insert_time'):
                continue
            new_data[k] = v
        dump_data.append(new_data)
    json.dump(dump_data, open('db/db_data_card', 'w'))


def dump_order():
    db = DBManager('report', 'order')
    all_data = db.find()
    dump_data = []
    for data in all_data:
        new_data = {}
        for k, v in data.items():
            if k in ('_id', 'insert_time'):
                continue
            new_data[k] = v
        dump_data.append(new_data)
    json.dump(dump_data, open('db/db_data_order', 'w'))


def write_order():
    db = DBManager('report', 'order')
    data_list = json.load(open('db/db_data_order'))
    for data in data_list:
        db.update_data(['order_id', 'client'], data)


def write_card():
    db = DBManager('report', 'card')
    data_list = json.load(open('db/db_data_card'))
    for data in data_list:
        data['process_id'] = int(data['process_id'])
        db.update_data(['order_id', 'process_id'], data)


def write_erp_dump_card(card):
    db = DBManager('report', 'card')
    # ["190529004", "2-1", null, 16.0, 2984.0, "R190529006"]
    all_ = {}
    for item in card:
        try:
            process_id = int(item[0])
        except Exception as e:
            continue
        data = {
            'process_id': process_id,
            'order_id': item[5],
            'total_pot_number': item[1],
            'card_type': item[2],
            'cloth_number': item[3],
            'cloth_length': item[4]
        }
        db.update_data(['order_id', 'process_id'], data)


def write_dump_erp_order(order):
    db = DBManager('report', 'order')
    # ["R190529005",
    # "", "R", "2019-05-29 14:02:24", "0156", "\u963f\u73ab",
    # "\u738b\u5bb6\u987a", "A0471", "10#\u7eac\u5f39\u63d0\u82b1", "AD190529001", "1#\u7ea2\u8272",
    # "120", "200", 100.0]
    all_ = {}
    for item in order:
        data = {
            'order_id': item[0],
            'client': item[5],
            'grey_cloth': item[8],
            'colour': item[10],
            'finished_product_width': item[11],
            'finished_product_gram_weight': item[12],
        }
        db.update_data(['order_id', 'client'], data)


def write_dump_erp_material(data):
    from write_erp_db import write_material
    write_material(data)


def dump_quality_statistics():
    db = DBManager('report', 'quality_statistics')
    all_data = db.find()
    dump_data = []
    for data in all_data:
        new_data = {}
        for k, v in data.items():
            if k in ('_id', 'insert_time'):
                continue
            new_data[k] = v
        new_data['day'] = new_data['day'].strftime('%Y-%m-%d')
        print(new_data)
        dump_data.append(new_data)
    json.dump(dump_data, open('db/db_data_card_quality_statistics', 'w'))

def write_quality_statistics():
    from datetime import datetime
    from datetime import timedelta
    from copy import deepcopy
    db = DBManager('report', 'quality_statistics')
    db.remove_all()
    data_list = json.load(open('db/db_data_card_quality_statistics'))
    new_data_list = []
    for data in data_list:
        data = deepcopy(data)
        day = data['day']
        if '-03-' in day:
            day = day.replace('-03-', '-05-')
        elif '-04-' in day:
            day = day.replace('-04-', '-06-')
        data['day'] = day
        new_data_list.append(data)
    for data in data_list:
        data = deepcopy(data)
        day = data['day']
        if '-03-' in day:
            day = day.replace('-03-', '-07-')
        elif '-04-' in day:
            day = day.replace('-04-', '-08-')
        if '-07-31' in day:
            data_more = deepcopy(data)
            data_more['day'] = day.replace('-07-', '-08-')
            new_data_list.append(data_more)
        data['day'] = day
        new_data_list.append(data)
    data_list.extend(new_data_list)
    for data in data_list:
        data['day'] = datetime.strptime(data['day'], "%Y-%m-%d")
    print(len(data_list), len(new_data_list))
    db.insert_data(data_list)


def dump_product_order():
    db = DBManager('report', 'product_order')
    all_data = db.find()
    dump_data = []
    for data in all_data:
        new_data = {}
        for k, v in data.items():
            if k in ('_id', 'insert_time'):
                continue
            new_data[k] = v
        new_data['day'] = new_data['day'].strftime('%Y-%m-%d')
        print(new_data)
        dump_data.append(new_data)
    json.dump(dump_data, open('db/db_data_product_order', 'w'))


def write_product_order():
    db = DBManager('report', 'product_order')
    data_list = json.load(open('db/db_data_product_order'))
    new_data_list = []
    print(len(data_list))
    for data in data_list:
        day = data['day']
        if '2019-03-31' <= day <= '2019-04-03':
            new_data = deepcopy(data)
            new_day = datetime.strptime(day, '%Y-%m-%d') + timedelta(days=62)
            new_data['day'] = new_day
            new_data_list.append(new_data)
        if day <= '2019-04-30':
            new_data = deepcopy(data)
            new_day = datetime.strptime(day, '%Y-%m-%d') + timedelta(days=31)
            new_data['day'] = new_day
            new_data_list.append(new_data)
    db.insert_data(new_data_list)


def view_data():
    data_list = json.load(open('db/erp_store'))
    out = {}
    for data in data_list:
        print(data)
    #     day = data[1][0:10]
    #     if day in out:
    #         out[day] += data[6]
    #     else:
    #         out[day] = data[6]
    #     # print(data[1], data[6])
    # out_list = sorted(out.items(), key=lambda  x: x[0])
    # for k, v in out_list:
    #     print(k, v)


if __name__ == '__main__':
    # # dump_card()
    # # dump_order()
    # import time
    # t1 = time.time()
    # write_order()
    # # write_card()
    # write_erp_dump_card(json.load(open('db/erp_m_card_order')))
    # write_dump_erp_order(json.load(open('db/erp_order')))
    # print(time.time() - t1)
    # view_data()
    # # write_dump_erp_material(json.load(open('db/erp_wuhao')))
    # dump_quality_statistics()
    # write_quality_statistics()
    # dump_product_order()
    write_product_order()
