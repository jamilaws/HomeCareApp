from base import base_logger, PeriodicTrigger, LocalGateway
from events import TrainOccupancyModelEventFabric

app = LocalGateway()

evt = TrainOccupancyModelEventFabric()

tgr = PeriodicTrigger(evt, "5m", "30s")
