import os
import json
import hashlib
import requests
import time
import logging
import traceback
from datetime import datetime

import settings
from db_connect import MsDbConn
from db_connect import MySQLDbConn
from action_data import ActionData
from processor import Processor


class KBProcess(Processor):

    def __init__(self):
        super(KBProcess, self).__init__()
        self.db = None # MsDbConn(settings.KB_HOST, settings.KB_USERNAME, settings.KB_PASSWORD, settings.KB_DATABASE)
        # self.db = MySQLDbConn(settings.T_KB_HOST, settings.T_KB_USERNAME, settings.T_KB_PASSWORD, settings.T_KB_DATABASE)
        self.pid = settings.KB_DB_PID
        self.format_datetime = "%Y-%m-%d %H:%M:%S"
        self.break_data = {}
        self.break_file = os.path.join(settings.BASE_FOLDER, 'var/kb_break.json')

    def load_break_data(self):
        self.break_data = json.load(open(self.break_file))
        logging.getLogger('status').info('%s' % json.dumps(self.break_data))

    def write_break_data(self):
        json.dump(self.break_data, open(self.break_file, 'w'))

    def reset_break_data(self, temperature_id, step_id, status_id):
        self.break_data = {
            't_temperature': temperature_id,
            't_devicehistorystep': step_id,
            't_devicehistorystatus': status_id
        }
        json.dump(self.break_data, open(self.break_file, 'w'))

    def dump_t_temperature(self):
        out_data = []
        break_key = 't_temperature'
        table_name = break_key
        max_id = self.break_data.get(break_key, 0)
        sql = " select a.*, b.mystatus from %s a join t_devicestaus b on a.myDeviceID = b.myid where a.myid > %s" % (table_name, max_id)
        db_data_list = self.db.query_all(sql)
        for data in db_data_list:
            data = list(data)
            data[0] = int(data[0])
            if data[0] > max_id:
                max_id = data[0]
            data[8] = data[8].strftime(self.format_datetime)
            out_data.append(data)
        if not out_data:
            return None
        self.break_data.update({break_key: max_id})
        d = ActionData(break_key, out_data, ActionData.ActionAdd, self.pid)
        return d.packet_action()

    def dump_t_devicehistorystep(self):
        out_data = []
        break_key = 't_devicehistorystep'
        table_name = break_key
        max_id = self.break_data.get(break_key, 0)
        sql = " select * from %s  where myid > %s" % (table_name, max_id)
        db_data_list = self.db.query_all(sql)
        for data in db_data_list:
            data = list(data)
            data[0] = int(data[0])
            if data[0] > max_id:
                max_id = data[0]
            out_data.append(data)
        if not out_data:
            return None
        self.break_data.update({break_key: max_id})
        d = ActionData(break_key, out_data, ActionData.ActionAdd, self.pid)
        return d.packet_action()

    def dump_t_devicehistorystatus(self):
        out_data = []
        break_key = 't_devicehistorystatus'
        table_name = break_key
        max_id = self.break_data.get(break_key, 0)
        sql = " select * from %s where myid > %s" % (table_name, max_id)
        db_data_list = self.db.query_all(sql)
        for data in db_data_list:
            data = list(data)
            data[0] = int(data[0])
            if data[0] > max_id:
                max_id = data[0]
            data[-1] = data[-1].strftime(self.format_datetime)
            data[-2] = data[-2].strftime(self.format_datetime)
            out_data.append(data)
        if not out_data:
            return None
        self.break_data.update({break_key: max_id})
        d = ActionData(break_key, out_data, ActionData.ActionAdd, self.pid)
        return d.packet_action()

    def dump_t_functionlist(self):
        out_data = []
        break_key = 't_functionlist'
        table_name = break_key
        hash_value = self.break_data.get(break_key, "")
        sql = " select * from %s" % table_name
        db_data_list = self.db.query_all(sql)
        for data in db_data_list:
            data = list(data)
            if isinstance(data[5], bytes):
                data[5] = data[5].decode(encoding='UTF-8')
            out_data.append(data)
        new_hash_value = hashlib.md5(str(out_data).encode()).hexdigest()
        if hash_value == new_hash_value:
            return None
        self.break_data.update({break_key: new_hash_value})
        d = ActionData(break_key, out_data, ActionData.ActionAll, self.pid)
        return d.packet_action()

    def dump_t_devicestaus(self):
        out_data = []
        break_key = 't_devicestaus'
        table_name = break_key
        hash_value = self.break_data.get(break_key, "")
        sql = " select * from %s" % table_name
        db_data_list = self.db.query_all(sql)
        for data in db_data_list:
            data = list(data)
            data[0] = int(data[0])
            if isinstance(data[18], datetime):
                data[18] = data[18].strftime(self.format_datetime)
            if isinstance(data[19], datetime):
                data[19] = data[19].strftime(self.format_datetime)
            if isinstance(data[24], bytes):
                data[24] = data[24].decode(encoding='UTF-8')
            out_data.append(data)
        new_hash_value = hashlib.md5(str(out_data).encode()).hexdigest()
        if hash_value == new_hash_value:
            return None
        self.break_data.update({break_key: new_hash_value})
        d = ActionData(break_key, out_data, ActionData.ActionAll, self.pid)
        return d.packet_action()

    def dump_t_devicestep(self):
        out_data = []
        break_key = 't_devicestep'
        table_name = break_key
        hash_value = self.break_data.get(break_key, "")
        sql = " select * from %s" % table_name
        db_data_list = self.db.query_all(sql)
        for data in db_data_list:
            data = list(data)
            data[0] = int(data[0])
            out_data.append(data)
        new_hash_value = hashlib.md5(str(out_data).encode()).hexdigest()
        if hash_value == new_hash_value:
            return None
        self.break_data.update({break_key: new_hash_value})
        d = ActionData(break_key, out_data, ActionData.ActionAll, self.pid)
        return d.packet_action()

    def process(self):
        logging.getLogger('agent').info("start sync kb data ...")
        self.db = MsDbConn(settings.KB_HOST, settings.KB_USERNAME, settings.KB_PASSWORD, settings.KB_DATABASE)
        self.load_break_data()
        post_data = self.collect_data()
        self.post_data(post_data)
        # logging.getLogger('agent').debug("post %s" % post_data)
        self.write_break_data()
        self.db.close()
        logging.getLogger('agent').info('over sync kb data...')

    def collect_data(self):
        content = []
        function_list = [
            self.dump_t_temperature,
            self.dump_t_devicehistorystatus,
            self.dump_t_devicehistorystep,
            self.dump_t_functionlist,
            self.dump_t_devicestaus,
            self.dump_t_devicestep
        ]
        for dump_function in function_list:
            try:
                out_data = dump_function()
            except Exception as e:
                logging.getLogger('agent').debug(traceback.format_exc())
                logging.getLogger('agent').error("process function %s, %s" % (e, dump_function))
            else:
                if not out_data:
                    logging.getLogger('agent').debug("out_data[%s] is none" % dump_function)
                    continue
                logging.getLogger('agent').debug("out_data: %s,  %s" % (dump_function, out_data))
                content.append(out_data)

        return {'ts': int(time.time()), 'data': content}

    def parse_response(self, content):
        return True

    def post_data(self, data, tries=3):
        for i in range(1, tries+1):
            try:
                req = requests.post(settings.DB_KB_URL, json=data)
            except Exception as err:
                logging.getLogger('agent').error('post kb_data error, %s, try num %i' % (err, i))
            else:
                content = req.content
                self.parse_response(req.content)
                logging.getLogger('agent').debug('post data, back data %s' % content)
                break
        else:
            raise Exception("has try %s nums, transfer maybe die, post kb_data error" % tries)

