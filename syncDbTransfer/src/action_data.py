
class ActionData():
    ActionNone = 5
    ActionAll = 0
    ActionAdd = 1

    def __init__(self, key, data, action, pid):
        self.key = key
        self.data = data
        self.action = action
        self.pid = pid

    def packet_action(self):
        return {
            "key": self.key,
            "data": self.data,
            "action": self.action,
            "pid": self.pid
        }


class CrossActionData(object):
    def __init__(self):
        self.trigger = False
        self.args = {}
