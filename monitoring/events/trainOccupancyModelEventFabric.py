from base import BaseEventFabric


class TrainOccupancyModelEventFabric(BaseEventFabric):

    def __init__(self):
        super(TrainOccupancyModelEventFabric, self).__init__()

    def call(self, *args, **kwargs):
        return "TrainOccupancyModelEvent", None
