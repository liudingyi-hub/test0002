from action_data import ActionData, CrossActionData

from real_time_update import write_device_temperature
from real_time_update import write_device_history_step
from real_time_update import write_device_history_info
from real_time_update import write_device_status
from real_time_update import write_device_step
from real_time_update import write_device_plan_temperature
from real_time_update import write_device_history_plan_temperature
from model.kb_history_status import KbHistoryStatus
from model.kb_history_step import KbHistoryStep

from write_erp_db import write_material
from write_erp_db import write_card
from write_erp_db import write_order
from write_erp_db import write_store


def event_function_default(*args):
    return True


class ActionEventKb(object):
    key_cross_plan = 't_plan_temperature'
    key_cross_history_plan = 't_history_plan_temperature'

    def __init__(self):
        self.action_cross_map = {
            ActionEventKb.key_cross_plan: CrossActionData(),
            ActionEventKb.key_cross_history_plan: CrossActionData()
        }

    @property
    def functions(self):
        return {
            't_temperature': write_device_temperature,
            't_devicehistorystatus': write_device_history_info,
            't_devicehistorystep': write_device_history_step,
            't_devicestaus': write_device_status,
            't_devicestep': write_device_step,
            't_functionlist': event_function_default,
        }

    def cross_data(self):
        action_cross_map = self.action_cross_map
        # 需要先执行history plan， 再执行plan
        if action_cross_map.get(ActionEventKb.key_cross_history_plan).trigger is True:
            kwargs = action_cross_map.get(ActionEventKb.key_cross_history_plan).args
            write_device_history_plan_temperature(**kwargs)
        if action_cross_map.get(ActionEventKb.key_cross_plan).trigger is True:
            kwargs = action_cross_map.get(ActionEventKb.key_cross_plan).args
            write_device_plan_temperature(**kwargs)

    def action(self, action_data):
        assert isinstance(action_data, ActionData)
        self.functions[action_data.key](action_data.data, pre_id=action_data.pid)
        cross_plan = self.action_cross_map[ActionEventKb.key_cross_plan]
        cross_history_plan = self.action_cross_map[ActionEventKb.key_cross_history_plan]
        if action_data.key == 't_devicestaus':
            cross_plan.trigger = True
            cross_plan.args['t_devicestaus'] = action_data.data
            cross_plan.args['pre_id'] = action_data.pid
        elif action_data.key == 't_devicestep':
            cross_plan.trigger = True
            cross_plan.args['t_devicestep'] = action_data.data
            cross_plan.args['pre_id'] = action_data.pid
        elif action_data.key == 't_devicehistorystatus':
            cross_history_plan.trigger = True
            cross_history_plan.args['t_devicehistorystaus'] = action_data.data
            cross_history_plan.args['pre_id'] = action_data.pid
            arg_date_time = KbHistoryStep.get_plan_arg_date(action_data.data, 2)
            old_arg_date_time = cross_history_plan.args.get('date_time')
            if old_arg_date_time is None or  arg_date_time < old_arg_date_time:
                cross_history_plan.args['date_time'] = arg_date_time
        elif action_data.key == 't_devicehistorystep':
            cross_history_plan.trigger = True
            cross_history_plan.args['t_devicehistorystep'] = action_data.data
            cross_history_plan.args['pre_id'] = action_data.pid
            arg_date_time = KbHistoryStatus.get_plan_arg_date(action_data.data, 2)
            old_arg_date_time = cross_history_plan.args.get('date_time')
            if old_arg_date_time is None or arg_date_time < old_arg_date_time:
                cross_history_plan.args['date_time'] = arg_date_time


class ActionEventErp(object):

    @property
    def functions(self):
        return {
            'erp_wuhao': write_material,
            'erp_order': write_order,
            'erp_store': write_store,
            'erp_m_card_order': write_card
        }

    def action(self, action_data):
        assert isinstance(action_data, ActionData)
        self.functions[action_data.key](action_data.data)

