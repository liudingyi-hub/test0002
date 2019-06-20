import logging
import traceback

import pymongo
from datetime import datetime, timedelta
from tools import get_process_id, DBManager, get_device_id


def write_card(card):
    db = DBManager('report', 'card')
    # ["190529004", "2-1", 16.0, 2984.0, "R190529006"]
    # ["190529004", "2-1", null, 16.0, 2984.0, "R190529006"]
    all_ = {}
    for item in card:
        try:
            process_id = int(item[0])
            kb_process_id = int(str(process_id)[1:])
        except Exception as e:
            process_id = item[0]
            kb_process_id = process_id
        card_t = {
            'process_id': process_id,
            'kb_process_id': kb_process_id,
            'order_id': item[5],
            'total_pot_number': item[1],
            'card_type': item[2],
            'cloth_number': item[3],
            'cloth_length': item[4]
        }
        db.update_data(['order_id', 'process_id'], card_t)
    #     all_[item[0]] = card_t
    # data = []
    # for key in all_:
    #     data.append(all_[key])
    # order_db.insert_data(sorted(data, key=lambda item: item['order_id']))


def write_order(order):
    db = DBManager('report', 'order')
    # ["R190529005", "2019-05-29 14:02:24", "0156", "\u963f\u73ab",
    # "\u738b\u5bb6\u987a", "A0471", "10#\u7eac\u5f39\u63d0\u82b1",
    # "AD190529001", "1#\u7ea2\u8272", "120", "200", 100.0]
    all_ = {}
    for item in order:
        order_t = {
            'order_id': item[0],
            'client': item[5],
            'grey_cloth': item[8],
            'colour': item[10],
            'finished_product_width': item[11],
            'finished_product_gram_weight': item[12],
        }
        db.update_data(['order_id', 'client'], order_t)
    #     all_[item[0]] = order_t
    # data = []
    # for key in all_:
    #     data.append(all_[key])
    # order_db.insert_data(sorted(data, key=lambda item: item['order_id']))


def write_material(item_list):
    db = DBManager('report', 'material_statistic')
    data_list = []
    for item in item_list:
        if item[9] != 3:
            continue
        data = {
            "day": datetime.strptime(item[0][:10], "%Y-%m-%d"),
            "store_out_no": item[1],
            "material_no": item[2],
            "material_name": item[3],
            "material_type_name": item[4],
            "store_out_qty": item[5],
            "unit": item[6],
            "price": item[7],
            "amount": item[8],
            "store_name": item[10],
        }
        data_list.append(data)
    data_list.sort(key=lambda x: x['day'])
    db.remove_all()
    db.insert_data(data_list)


def write_store(item_list):
    db_day = DBManager('report', 'product_order')
    # db_detail =  DBManager('report', 'product_order')
    day_product = {}
    for item in item_list:
        day = item[1][0:10]
        key = '%s_%s' % (day, item[0])
        if key in day_product:
            day_product[key]['amount'] += item[6]
        else:
            day_product[key] = {'day': datetime.strptime(day, '%Y-%m-%d'), 'order_id': item[0], 'amount': item[6]}
    for product_info in day_product.values():
        db_day.update_data(['day', 'order_id'], product_info)




