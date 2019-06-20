# coding=UTF-8
from pymongo import MongoClient
from datetime import datetime,timedelta



# before 2019-03-01
# 增加键值显示

# table.update({'_id', data_ori['_id']}, data_ori, upsert=True)
def update_steam():
    Mar_31 = '2019-03-31'
    date_Mar_31 = datetime.strptime(Mar_31, '%Y-%m-%d')
    new_data_list = []


    medium_pressure_steam_price = 230
    low_pressure_steam_price = 170

    mongo_uri = 'mongodb://%s:%s' % ("192.168.3.216", 27017)
    conn = MongoClient(mongo_uri)
    db = conn['report']
    table = db['energy_consumption']

    data_list_ori = table.find()
    for data_ori in data_list_ori:
        print(data_ori)
        if 'medium_pressure_steam_volume' in data_ori:
            data_ori["medium_pressure_steam_cost"] = str(int(data_ori['medium_pressure_steam_volume']) * medium_pressure_steam_price)
        if 'low_pressure_steam_volume' in data_ori:
            data_ori["low_pressure_steam_cost"] = str(int(data_ori['low_pressure_steam_volume']) * low_pressure_steam_price)
        table.update({'_id': data_ori['_id']}, {'$set': data_ori},   upsert=True)

#
# for data_ori in data_list_ori:
#     origin_data={}
#     for k in data_ori:
#         if k in ['_id']:
#             continue
#         else:
#             origin_data[k] = data_ori[k]
#     new_data_list.append(origin_data)
#
# #已有数据3、4月份，需求添加5，6，7，8月份数据
# counts=[1,2]
# for times in counts:
#     data_list_new_1 = table.find()
#     for data_ori in data_list_new_1:
#         insert_data = {}
#         for k in data_ori:
#             if k in ['_id']:
#                 continue
#             elif k == 'day':
#                 if times == 1:
#                     insert_data['day'] = data_ori['day'] + timedelta(days=61)  # add 61 days(31+30)
#                 else:
#                     insert_data['day'] = data_ori['day'] + timedelta(days=122)  # add 122 days(2*(31+30))
#             else:
#                 insert_data[k] = data_ori[k]
#         new_data_list.append(insert_data)
#
# # 找最后一天数据加入
# data_list_ori = table.find()
# for data_ori in data_list_ori:
#     insert_data = {}
#     find_mar_flag = 0
#     for k in data_ori:
#         if k in ['_id']:
#             continue
#         elif k == 'day':
#             if data_ori['day'] == date_Mar_31:
#                 insert_data['day'] = data_ori['day'] + timedelta(days = 61 + 61 + 31)  # add 123 days (61+61+31)
#                 find_mar_flag = 1
#         else:
#             insert_data[k] = data_ori[k]
#     if find_mar_flag == 1:
#         new_data_list.append(insert_data)
#
# print("Finish!")

if __name__ == '__main__':
    update_steam()