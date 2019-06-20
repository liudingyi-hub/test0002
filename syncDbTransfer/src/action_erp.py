import logging
import traceback

from action_data import ActionData
from action_events import ActionEventErp


class ActionErp(object):

    def __init__(self):
        self.action_event = ActionEventErp()

    def process(self, req_data):
        content = req_data['data']
        msg = ''
        if not content:
            return msg
        for element in content:
            try:
                self.process_action(element)
            except Exception as e:
                logging.getLogger('transfer').error(traceback.format_exc())
                logging.getLogger('transfer').error(e)

    def process_action(self, element):
        data_action = ActionData(element['key'], element['data'], element['action'], element['pid'])
        self.action_event.action(data_action)

