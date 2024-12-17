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

def sendEmail():
    base_logger.info("Sending email not yet implemented")

    return


async def emergencyNotificationFunction(request: Request):
    base_logger.info("Emergency notification function invoked")
    info = await request.json()
    emergency_data = info.get("EmergencyEvent", {}).get("data", {})
    base_logger.info(f"Received emergency notification: {emergency_data}")
    base_logger.info("Handeling Emergency")
    sendTodoToViz(emergency_data["room"].upper(), emergency_data["message"], emergency_data["level"]) 
    sendEmail()
    base_logger.info("Emergency handled")
    return

app.deploy(emergencyNotificationFunction, "emergencyNotificationFunction-fn", "EmergencyEvent", "POST")