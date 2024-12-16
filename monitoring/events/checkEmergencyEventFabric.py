from base import BaseEventFabric


class CheckEmergencyEventFabric(BaseEventFabric):

    def __init__(self):
        super(CheckEmergencyEventFabric, self).__init__()

    def call(self, *args, **kwargs):
        return "CheckEmergencyEvent", None