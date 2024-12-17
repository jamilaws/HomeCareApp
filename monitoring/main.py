import os
from datetime import timedelta
from fastapi import Request
import pandas as pd
import datetime
import time
import urllib3

from influxdb_client import InfluxDBClient
from minio import Minio
from io import BytesIO
import json

from events import TrainOccupancyModelEventFabric, EmergencyEventFabric, CheckEmergencyEventFabric, AnalyseMotionEventFabric
from base import base_logger, PeriodicTrigger, LocalGateway, OneShotTrigger

app = LocalGateway()

BUCKET_KITCHEN = "1_2_9"
BUCKET_BATHROOM = "1_3_6"
BUCKET_ROOM = "1_2_2"
BUCKET_DOOR = "1_4_8"
BUCKETS_PIR = [BUCKET_KITCHEN, BUCKET_BATHROOM, BUCKET_ROOM, BUCKET_DOOR]

INFLUX_ORG = "wise2024"
INFLUX_TOKEN = os.environ.get("INFLUXDB_HOST", "192.168.178.63:8086")
INFLUX_USER = os.environ.get("INFLUXDB_USER", "admin")
INFLUX_PASS = os.environ.get("INFLUXDB_PASS", "secure_influx_iot_user")
SIF_SCHEDULER = os.environ.get("SCH_SERVICE_NAME", "192.168.178.63:30032")

MINIO_SERVER = "192.168.178.63:9090"
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "DjcfggrfVXj5zaLJ")
MINIO_BUCKET = "models"

VIZ_URL = "http://192.168.178.63:9000"

if INFLUX_TOKEN is None or INFLUX_USER is None or INFLUX_PASS is None or SIF_SCHEDULER is None:
    raise ValueError("Missing env variables")
base_logger.info("all env variables are there")
print("all env variables are there")

app = LocalGateway(localhost="192.168.178.61") # localhost is the value of your laptop's IP address
                                              # you can leave it here even when making the docker image
                                              # for k3s as gateway.py sets automatically the IP address if
                                              # it detects that it runs in k3s.



def fetch_data(bucket, measurement):
    with InfluxDBClient(url=INFLUX_TOKEN, org=INFLUX_ORG, username=INFLUX_USER, password=INFLUX_PASS, verify_ssl=False) as client:
        base_logger.info(f"Fetching data from {bucket}")
        p = {
            "_start": timedelta(days=-7),
        }

        query_api = client.query_api()
        tables = query_api.query(f'''
                                 from(bucket: "{bucket}") |> range(start: _start)
                                |> filter(fn: (r) => r["_measurement"] == "{measurement}")
                                 ''', params=p)
        obj = []
        for table in tables:
            for record in table.records:
                val = {}
                val["bucket"] = bucket
                val["timestamp"] = record["_time"].timestamp() * 1000
                if len(val.keys()) != 0:
                    obj.append(val)
        return obj


def fetch_model():
    client = Minio(MINIO_SERVER, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    # fetching the name of the latest model
    try:
        model_names = client.get_object(MINIO_BUCKET, "latest_models.json")
        names = model_names.read().decode(encoding="utf-8")
        loaded_names = json.loads(names)
        latest = loaded_names["latest"]
    except Exception as e:
        base_logger.error(f"Failed to fetch the name of the latest model from minio: {e}")
    try:
        model = client.get_object(MINIO_BUCKET, latest) 
        model_data = model.read()
    except Exception as e:
        base_logger.error(f"Failed to fetch model from minio: {e}")
    return model_data

def fetchLastTwoModels():
    client = Minio(MINIO_SERVER, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    # fetching the name of the latest model
    try:
        model_names = client.get_object(MINIO_BUCKET, "latest_models.json")
        names = model_names.read().decode(encoding="utf-8")
        loaded_names = json.loads(names)
        latest = loaded_names["latest"]
        previous = loaded_names["before"]
    except Exception as e:
        base_logger.error(f"Failed to fetch the name of the latest model from minio: {e}")
    try:
        latest_model = client.get_object(MINIO_BUCKET, latest) 
        latest_model_data = latest_model.read()
        previous_model = client.get_object(MINIO_BUCKET, previous)
        previous_model_data = previous_model.read()
    except Exception as e:
        base_logger.error(f"Failed to fetch model from minio: {e}")
    return latest_model_data, previous_model_data

def fetch_influx_data():
    all_data = []
    for bucket in BUCKETS_PIR:
        if bucket == BUCKET_KITCHEN:
            data = fetch_data(bucket, "kitchen")
        else:
            data = fetch_data(bucket, "PIR")
        all_data.extend(data)

    df = pd.DataFrame(all_data)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit='ms')
    df = df.sort_values('timestamp')

    return df

def check_emergency(data, model):
    base_logger.info("Checking for emergency")

    # Read and parse the JSON from the BytesIO object
    model_json = model.decode(encoding="utf-8")
    model_data = json.loads(model_json)

    # Validate the structure of the model data
    if not all(key in model_data for key in ["bucket", "time_interval", "average_duration_minutes"]):
        base_logger("Model JSON does not have the required keys.")

    bucket_mapping = {
        "1_2_9": "kitchen",
        "1_3_6": "bathroom",
        "1_2_2": "bedroom",
        "1_4_8": "door",
    }

    base_logger.info("data of past 6 hours:" + str(data))
    base_logger.info("latest model data: " + str(model_data))

    if len(data) == 0:
        return True, "Sensors failed; there might be an undetected emergency", "general", 0

    data["room"] = data["bucket"].replace(bucket_mapping)
    data = data.sort_values(by="timestamp").reset_index(drop=True)
    data["stay_id"] = (data["room"] != data["room"].shift()).cumsum()
    # Get the last stay (all others are irrelevant as the pearson clearly moved)
    last_stay = data[data["stay_id"] == data["stay_id"].iloc[-1]]
    room = last_stay["room"].iloc[0]
    if room == "door":
        return False, "No emergency detected. Patient is not at home.", "general", 0
    start_time = last_stay["timestamp"].iloc[0]
    end_time = last_stay["timestamp"].iloc[-1]
    current_duration = (end_time - start_time).total_seconds() / 60

    hour = end_time.hour
    if 0 <= hour < 6:
        time_interval = "00-06"
    elif 6 <= hour < 12:
        time_interval = "06-12"
    elif 12 <= hour < 17:
        time_interval = "12-17"
    else:
        time_interval = "17-00"
    
    # Find the average duration for the room and timespan from the model
    avg_duration = None
    for i in range(len(model_data["bucket"])):
        if model_data["bucket"][str(i)] == room and model_data["time_interval"][str(i)] == time_interval:
            avg_duration = model_data["average_duration_minutes"][str(i)]
            break
    
    if avg_duration is None:
        return True, f"No model data available for the current room {room} and time interval {time_interval}" , room, 0
    
    # Check if the current duration is 20% longer than the average
    threshold_level_2 = avg_duration * 1.2
    threshold_level_1 = avg_duration * 1.1
    is_longer = current_duration > threshold_level_2
    is_longer_level_1 = current_duration > threshold_level_1

    if is_longer:
        return True, f"Emergency detected in {room}. Patient has been in the room for {current_duration} minutes, which is at least 20% longer than the average of {avg_duration} minutes", room, 2
    elif is_longer_level_1:
        return True, f"Emergency detected in {room}. Patient has been in the room for {current_duration} minutes, which is at least 10% longer than the average of {avg_duration} minutes", room, 1
   
    return False, f"No emergency detected in {room}. Patient has been in the room for {current_duration} minutes, which is within the normal range of {avg_duration} minutes", room, 0

def analyseMotionDifference(latestModelBytes, previousModelBytes):
    latestModel = json.loads(latestModelBytes)
    previousModel = json.loads(previousModelBytes)

    level = 0
    details_3 = []
    details_2 = []
    details_1 = []

    buckets = latestModel["bucket"]
    time_intervals = latestModel["time_interval"]

    for key in buckets.keys():
        bucket = buckets[key]
        time_interval = time_intervals[key]

        if bucket not in previousModel["bucket"] or time_interval not in previousModel["time_interval"]:
            continue

        avg_duration_latest = latestModel["average_duration_minutes"][key]
        avg_duration_previous = previousModel["average_duration_minutes"][key]

        # Check how much in percentage the average duration has changed
        change = abs((avg_duration_latest - avg_duration_previous) / avg_duration_previous) * 100

        if change > 50:
            level = max(level, 3)
        elif change > 30:
            level = max(level, 2)
        elif change > 10:
            level = max(level, 1)

    details = ""
    if len(details_3) > 0:
        details.append(f"The patient significantly changed the behaviour in the rooms: " + ", ".join(details_3) + "\n")
    if len(details_2) > 0:
        details.append(f"The patient changed the behaviour in the rooms: " + ", ".join(details_2)+  "\n")
    if len(details_1) > 0:
        details.append(f"The patient slightly changed the behaviour in the rooms: " + ", ".join(details_1)+  "\n")

    if level == 0:
        details = "No significant changes in the patient's behaviour"

    return level, details

def sendInformationToVIZ(level, details):
    info = {
        "summary": "Motion analysis",
        "detail": details,
        "level": level,
        "timestamp": int(time.time()*1000)
    }

    infoEncoded = json.dumps(info).encode('utf-8')
    http = urllib3.PoolManager()
    base_logger.info("Sending info to VIZ")
    try:
        response = http.request('POST', VIZ_URL + "/api/info", body=infoEncoded, headers={'Content-Type': 'application/json'})
 
    except Exception as e:
        base_logger.error(f"Error sending info to VIZ: {e}")

    if response.status == 200:
        base_logger.info(f"Info successfully sent")
    else:
        base_logger.error(f"Failed to send info. Status: {response.status}, Response: {response.data.decode('utf-8')}")

    return

async def emergencyDetectionFunction():
    base_logger.info("Emergency detection function called")
    base_logger.info("Fetching model from minio")
    model = fetch_model()
    base_logger.info("Fetching data for past 6 hours")
    data = fetch_influx_data()
    base_logger.info("Checking for emergancy")
    emergency, message, room, level = check_emergency(data, model)
    if emergency:
        base_logger.info(message)
        base_logger.info("room: " + room)
        base_logger.info("level: " + str(level))
        emergencyEvent = EmergencyEventFabric(room=room, level=level, message=message)
        emergencyTrigger = OneShotTrigger(emergencyEvent)
    else:
        base_logger.info(message)

    return 

async def motionAnalysisFunction():
    base_logger.info("Motion analysis function called")
    lastModel, previousModel = fetchLastTwoModels()
    base_logger.info("Analysing differences between the last two models")
    level, details = analyseMotionDifference(lastModel, previousModel)
    base_logger.info(f"Level: {level}, Details: {details}")
    sendInformationToVIZ(level, details)
    return

app.deploy(emergencyDetectionFunction, "emergencyDetectionFunction-fn", "CheckEmergencyEvent")
app.deploy(motionAnalysisFunction, "motionAnalysisFunction-fn", "AnalyseMotionEvent")

evt = TrainOccupancyModelEventFabric()
evtCheckEmergency = CheckEmergencyEventFabric()
evtAnalyseMotion = AnalyseMotionEventFabric()


tgr = PeriodicTrigger(evt, "24h", "30s")
tgrCheckEmergency = PeriodicTrigger(evtCheckEmergency, "30m", "30s") 
tgrAnalyseMotion = PeriodicTrigger(evtAnalyseMotion, "24h", "30s")