from base import base_logger, PeriodicTrigger
from events import TrainOccupancyModelEventFabric

evt = TrainOccupancyModelEventFabric()

tgr = PeriodicTrigger(evt, "30s", "1m")
