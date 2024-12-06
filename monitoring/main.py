from base import LocalGateway, base_logger, PeriodicTrigger
from events import TrainOccupancyModelEvent

evt = TrainOccupancyModelEvent()

tgr = PeriodicTrigger(evt, "30s", "1m")
