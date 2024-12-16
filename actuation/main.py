import os
import time
import urllib3
from fastapi import Request
import json
from base import LocalGateway, base_logger, PeriodicTrigger, ExampleEventFabric, OneShotTrigger, BaseEventFabric

VIZ_URL = "http://192.168.178.63:9000"


app = LocalGateway()

def sendTodoToViz(title, message, level):
    todo = {
        "titel": title,
        "msg": message,
        "level": level,
        "timestamp": int(time.time()*1000)
    }

    todoEncoded = json.dumps(todo).encode('utf-8')
    http = urllib3.PoolManager()
    base_logger.info("Sending todo to VIZ")
    try:
        response = http.request('POST', VIZ_URL + "/api/todo", body=todoEncoded, headers={'Content-Type': 'application/json'})
 
    except Exception as e:
        base_logger.error(f"Error sending todo to VIZ: {e}")

    if response.status == 200:
        base_logger.info(f"Todo successfully sent")
    else:
        base_logger.error(f"Failed to send todo. Status: {response.status}, Response: {response.data.decode('utf-8')}")

    return


def emergencyNotificationFunction(request: Request):
    info = await request.json()
    base_logger.info("Handeling Emergency")
    sendTodoToViz("Emergency", "Emergency detected", 2)
    base_logger.info("Emergency handled")
    return

app.deploy(emergencyNotificationFunction, "emergencyNotificationFunction-fn", "EmergencyEvent")

class EmergencyEventFabric(BaseEventFabric):

    def __init__(self):
        super(EmergencyEventFabric, self).__init__()

    def call(self, *args, **kwargs):
        return "EmergencyEvent", None

evt = EmergencyEventFabric()
trigger = OneShotTrigger(evt, "10s")