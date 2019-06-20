import os
import re
import json
import shutil
import traceback
import time
import tornado
import tornado.ioloop
import tornado.web
import logging
from concurrent.futures import ThreadPoolExecutor

import settings

g_web_thread_pool = ThreadPoolExecutor(4)


def web_server(ip, port):
    app = tornado.web.Application([
        (r"/", WebRootHandler),
        (r"/db/kb/", WebSyncKbHandler),
        (r"/db/erp/", WebSyncERPHandler),
    ])
    app.listen(port, address=ip)
    try:
        tornado.ioloop.IOLoop.instance().start()
    except Exception as err:
        logging.getLogger('transfer').debug(traceback.format_exc())
        logging.getLogger('transfer').error(err)


class WebRootHandler(tornado.web.RequestHandler):
    def get(self):
        try:
            self.write('transfer is alive\r\n')
            self.finish()
        except Exception as err:
            logging.getLogger('transfer').debug(traceback.format_exc())
            logging.getLogger('transfer').error(err)


class WebSyncKbHandler(tornado.web.RequestHandler):
    def post(self, *args, **kwargs):
        try:
            try:
                logging.getLogger('transfer').debug(self.request.body)
                req_data = json.loads(self.request.body)
            except Exception as err:
                logging.getLogger('transfer').debug(traceback.format_exc())
                self.asyn_send("request not json data, %s" % err)
            else:
                try:
                    data = self.process_data(req_data)
                    self.asyn_send(data)
                except Exception as err:
                    data = str(err)
                    logging.getLogger('transfer').debug(traceback.format_exc())
                    logging.getLogger('transfer').error("apiserver error, %s", err)
                self.asyn_send(data)
        except Exception as err:
            self.asyn_send(json.dumps({"status": 1, "msg": "handler process error"}))
            logging.getLogger('transfer').error("apiserver error, %s", err)

    def process_data(self, req_data):
        if req_data['key'] not in settings.SECRET_LIST:
            raise Exception('SERET ERROR')
        else:
            data_folder = settings.DATA_FOLDER
            his_folder = settings.DATA_HISTORY_FOLDER
            min_file = None
            for file_name in os.listdir(settings.DATA_FOLDER):
                if re.match(r'^kb_\d+$', file_name):
                    if min_file is None or file_name < min_file:
                        min_file = file_name
            if min_file is None:
                return ''
            fr = open(os.path.join(data_folder, min_file), 'r')
            data = fr.read()
            fr.close()
            shutil.move(os.path.join(data_folder, min_file), os.path.join(his_folder, min_file))
            return data

    def asyn_send(self, data):
        try:
            self.write(data)
            self.finish()
        except Exception as err:
            logging.getLogger('transfer').debug(traceback.format_exc())
            logging.getLogger('transfer').error("apiserver write socket error, %s" % err)


class WebSyncERPHandler(WebSyncKbHandler):

    def process_data(self, req_data):
        if req_data['key'] not in settings.SECRET_LIST:
            raise Exception('SERET ERROR')
        else:
            data_folder = settings.DATA_FOLDER
            his_folder = settings.DATA_HISTORY_FOLDER
            min_file = None
            for file_name in os.listdir(settings.DATA_FOLDER):
                if re.match(r'^erp_\d+$', file_name):
                    if min_file is None or file_name < min_file:
                        min_file = file_name
            if min_file is None:
                return ''
            fr = open(os.path.join(data_folder, min_file), 'r')
            data = fr.read()
            fr.close()
            shutil.move(os.path.join(data_folder, min_file), os.path.join(his_folder, min_file))
            return data




class ApiServer(object):
    IS_START = False

    def process(self):
        logging.getLogger('transfer').info("api server start ...")
        if not ApiServer.IS_START:
            ApiServer.IS_START = True
            web_server(settings.API_BIND_IP, settings.API_PORT)
        logging.getLogger('transfer').info("api server stop ... you never see me lucky")

