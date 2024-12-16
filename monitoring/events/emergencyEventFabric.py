from base import BaseEventFabric


class EmergencyEventFabric(BaseEventFabric):

    def __init__(self):
        super(EmergencyEventFabric, self).__init__()

    def call(self, *args, **kwargs):
        return "EmergencyEvent", None