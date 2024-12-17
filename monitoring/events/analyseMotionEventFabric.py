from base import BaseEventFabric


class AnalyseMotionEventFabric(BaseEventFabric):

    def __init__(self):
        super(AnalyseMotionEventFabric, self).__init__()

    def call(self, *args, **kwargs):
        return "AnalyseMotionEvent", None