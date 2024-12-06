import os
from monitoring.base.event import BaseEventFabric


class TrainOccupancyModelEvent(BaseEventFabric):

    def __init__(self):
        super(TrainOccupancyModelEvent, self).__init__()

    def call(self, *args, **kwargs):
        return "TrainOccupancyModelEvent", None
