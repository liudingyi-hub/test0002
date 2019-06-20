import os
import sys
import time
from threading import Thread
import logging.config
import logging

import settings
from daemonize import Daemon
from process_day import DayProcess
from process_minute import MinuteProcess
from api_server import ApiServer


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
        app_name="SYNC_DB_TRANSFER",
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


def process():
    th_list = [
        Thread(target=MinuteProcess().run),
        Thread(target=DayProcess().run)
    ]
    for th in th_list:
        th.setDaemon(True)
    for th in th_list:
        th.start()
    ApiServer().run()
    for th in th_list:
        th.join()


def main():
    init()
    run_daemon()
    process()


if __name__ == '__main__':
    main()
