import os
import sys
import time
from threading import Thread
import logging.config
import logging

import settings
from daemonize import Daemon
from api_server import ApiServer
from process_day import DayProcess
from process_minute import MinuteProcess


def run_daemon():
    version = '0.1.0'
    usage = '''
    Usage:
        agent (start|stop|status)
        agent help
        agent version
    '''
    argv = sys.argv
    if len(argv) != 2 or not argv[1] in ['start', 'stop', 'status', 'help', 'version']:
        print(usage)
        exit(0)
    daemon = Daemon(
        app_name="TMP_TRANSFER",
        pid_file=os.path.join(settings.BASE_FOLDER, settings.PID_FILE),
        stdout=settings.STDOUT,
        stderr=settings.STDERR
    )
    cmd = argv[1]
    if cmd == 'start':
        if not settings.DEBUG:
            daemon.start()
        return
    elif cmd == "stop":
        daemon.stop()
        exit(0)
    elif cmd == "status":
        daemon.status()
        exit(0)
    elif cmd == "help":
        print(usage)
        exit(0)
    elif cmd == "version":
        print(version)
        exit(0)


def init():
    settings.BASE_FOLDER = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    logging.config.fileConfig(os.path.join(settings.BASE_FOLDER, "conf/log_transfer.conf"))



import requests
import json
import traceback
from processor import Processor
from action_kb import ActionKb
from action_erp import ActionErp


class TmpProcess(Processor):
    key = '4953ab6a1abf94191d65704d300d7799'
    url_info = {'kb': 'http://123.59.26.197:8200/db/kb/', 'erp': 'http://123.59.26.197:8200/db/erp/'}

    def process(self):
        try:
            data = {'key': TmpProcess.key}
            for db_name, url in TmpProcess.url_info.items():
                req = requests.post(url, json=data)
                if req.status_code != 200:
                    return
                if not req.content:
                    return
                logging.getLogger('transfer').debug("req data: %s" % req.content)
                req_data = json.loads(req.content)
                if db_name == 'kb':
                    msg = ActionKb().process(req_data)
                elif db_name == 'erp':
                    msg = ActionErp().process(req_data)
            logging.getLogger('transfer').info("load data, result %s", msg)
        except Exception as e:
            logging.getLogger('transfer').debug(traceback.format_exc())
            logging.getLogger('transfer').error(str(e))



from threading import Thread
def process():
    th_list = [
        Thread(target=MinuteProcess().run),
        Thread(target=DayProcess().run),
        Thread(target=TmpProcess().run)
    ]
    for th in th_list:
        th.setDaemon(True)
    for th in th_list:
        th.start()
    for th in th_list:
        th.join()


def main():
    init()
    run_daemon()
    process()



if __name__ == '__main__':
    main()
