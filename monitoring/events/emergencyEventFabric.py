from base import BaseEventFabric


class EmergencyEventFabric(BaseEventFabric):

    def __init__(self, room=None, level=None, message=None):
        super(EmergencyEventFabric, self).__init__()
        self.room = room
        self.level = level
        self.message = message

    def call(self, *args, **kwargs):
        payload = {
            "room": self.room,
            "level": self.level,
            "message": self.message
        }
        return "EmergencyEvent", payload