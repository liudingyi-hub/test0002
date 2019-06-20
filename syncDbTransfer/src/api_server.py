import os
import json
import traceback
import time
import tornado
import tornado.ioloop
import tornado.web
import logging
from concurrent.futures import ThreadPoolExecutor

import settings
from processor import Processor
from action_kb import ActionKb
from action_erp import ActionErp

g_web_thread_pool = ThreadPoolExecutor(4)


def web_server(ip, port):
    app = tornado.web.Application([
        (r"/", WebRootHandler),
        (r"/db/kb/", WebSyncKbHandler),
        (r"/db/erp/", WebSyncErpHandler),
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
            result = {"status": 0, "msg": ""}
            try:
                logging.getLogger('transfer').debug(self.request.body)
                req_data = json.loads(self.request.body)
                self.dump_req_data(req_data['ts'], self.request.body)
            except Exception as err:
                logging.getLogger('transfer').debug(traceback.format_exc())
                result['status'],  result['msg'] = 2, "request not json data, %s" % err
            else:
                try:
                    msg = ActionKb().process(req_data)
                    if msg:
                        result = {"status": 3, "msg": msg}
                except Exception as err:
                    result = {"status": 3, "msg": str(err)}
                    logging.getLogger('transfer').debug(traceback.format_exc())
                    logging.getLogger('transfer').error("apiserver error, %s", err)
            self.asyn_send(json.dumps(result))
        except Exception as err:
            self.asyn_send(json.dumps({"status": 1, "msg": "handler process error"}))
            logging.getLogger('transfer').error("apiserver error, %s", err)

    def dump_req_data(self, ts, req_body):
        file_path = os.path.join(os.path.join(settings.BASE_FOLDER, 'data'), 'kb_%s' % ts)
        fw = open(file_path, 'w')
        if isinstance(req_body, bytes):
            fw.write(req_body.decode(encoding='UTF-8'))
        elif isinstance(req_body, str):
            fw.write(req_body)
        fw.close()

    def asyn_send(self, data):
        try:
            self.write(data)
            self.finish()
        except Exception as err:
            logging.getLogger('transfer').debug(traceback.format_exc())
            logging.getLogger('transfer').error("apiserver write socket error, %s" % err)


class WebSyncErpHandler(tornado.web.RequestHandler):
    def post(self, *args, **kwargs):
        try:
            result = {"status": 0, "msg": ""}
            try:
                logging.getLogger('transfer').debug(self.request.body)
                req_data = json.loads(self.request.body)
                self.dump_req_data(req_data['ts'], self.request.body)
            except Exception as err:
                logging.getLogger('transfer').debug(traceback.format_exc())
                result['status'],  result['msg'] = 2, "request not json data, %s" % err
            else:
                try:
                    msg = ActionErp().process(req_data)
                    if msg:
                        result = {"status": 3, "msg": msg}
                except Exception as err:
                    result = {"status": 3, "msg": str(err)}
                    logging.getLogger('transfer').debug(traceback.format_exc())
                    logging.getLogger('transfer').error("apiserver error, %s", err)
            self.asyn_send(json.dumps(result))
        except Exception as err:
            self.asyn_send(json.dumps({"status": 1, "msg": "handler process error"}))
            logging.getLogger('transfer').error("apiserver error, %s", err)

    def dump_req_data(self, ts, req_body):
        file_path = os.path.join(os.path.join(settings.BASE_FOLDER, 'data'), 'erp_%s' % ts)
        fw = open(file_path, 'w')
        if isinstance(req_body, bytes):
            fw.write(req_body.decode(encoding='UTF-8'))
        elif isinstance(req_body, str):
            fw.write(req_body)
        fw.close()

    def asyn_send(self, data):
        try:
            self.write(data)
            self.finish()
        except Exception as err:
            logging.getLogger('transfer').debug(traceback.format_exc())
            logging.getLogger('transfer').error("apiserver write socket error, %s" % err)


class ApiServer(Processor):
    IS_START = False

    def __init__(self):
        super(ApiServer, self).__init__()

    def process(self):
        logging.getLogger('transfer').info("api server start ...")
        if not ApiServer.IS_START:
            ApiServer.IS_START = True
            web_server(settings.API_BIND_IP, settings.API_PORT)
        logging.getLogger('transfer').info("api server stop ...")

