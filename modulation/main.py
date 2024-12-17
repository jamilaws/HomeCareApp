import os
from datetime import timedelta
from fastapi import Request
import pandas as pd
import datetime
import json

from influxdb_client import InfluxDBClient
from minio import Minio
from io import BytesIO
from base import LocalGateway, base_logger, BaseEventFabric, PeriodicTrigger

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


def find_averages(data):
    # Step 1: Sort the data by timestamp
    data = data.sort_values(by="timestamp").reset_index(drop=True)
    
    # Step 2: Identify consecutive stays in the same bucket
    data["stay_id"] = (data["bucket"] != data["bucket"].shift()).cumsum()
    
    # Step 3: Calculate start, end, and duration for each stay
    stays = data.groupby(["stay_id", "bucket"]).agg(
        start=("timestamp", "min"),
        end=("timestamp", "max")
    ).reset_index()
    
    # Calculate duration for each stay
    stays["duration"] = (stays["end"] - stays["start"]).dt.total_seconds() / 60  # Duration in minutes
    
    # Step 4: Calculate the average duration per bucket
    average_duration = stays.groupby("bucket")["duration"].mean().reset_index()
    average_duration.columns = ["bucket", "average_duration_minutes"]

    base_logger.info(average_duration)
    return average_duration

def find_averages_by_time_interval(data):

    bucket_mapping = {
        "1_2_9": "kitchen",
        "1_3_6": "bathroom",
        "1_2_2": "bedroom",
        "1_4_8": "door",
    }
    
    data["bucket"] = data["bucket"].replace(bucket_mapping)

    # Step 1: Sort the data by timestamp
    data = data.sort_values(by="timestamp").reset_index(drop=True)
    
    # Step 2: Identify consecutive stays in the same bucket
    data["stay_id"] = (data["bucket"] != data["bucket"].shift()).cumsum()
    
    # Step 3: Add a time interval column
    def get_time_interval(timestamp):
        hour = timestamp.hour
        if 0 <= hour < 6:
            return "00-06"
        elif 6 <= hour < 12:
            return "06-12"
        elif 12 <= hour < 17:
            return "12-17"
        else:
            return "17-00"
    
    data["time_interval"] = data["timestamp"].apply(get_time_interval)
    
    # Step 4: Calculate start, end, and duration for each stay
    stays = data.groupby(["stay_id", "bucket", "time_interval"]).agg(
        start=("timestamp", "min"),
        end=("timestamp", "max")
    ).reset_index()
    
    # Calculate duration for each stay
    stays["duration"] = (stays["end"] - stays["start"]).dt.total_seconds() / 60  # Duration in minutes
    
    # Step 5: Calculate the average duration per bucket and time interval
    average_duration = stays.groupby(["bucket", "time_interval"])["duration"].mean().reset_index()
    average_duration.columns = ["bucket", "time_interval", "average_duration_minutes"]
    
    base_logger.info(average_duration)
    return average_duration


def save_model(model):
    client = Minio(MINIO_SERVER, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)

    found = client.bucket_exists(MINIO_BUCKET)
    if not found:
        client.make_bucket(MINIO_BUCKET)

    current_time = datetime.datetime.now().strftime("%d-%m-%y_%H-%M-%S")
    data = pd.DataFrame.to_json(model).encode("utf-8")

    # save model to minio
    try:
        client.put_object(
            bucket_name=MINIO_BUCKET,
            object_name=f"model_{current_time}.json",
            data=BytesIO(data),
            length=len(data),
            content_type="application/json",
            metadata={'time': current_time}
        )
    except Exception as e:
        base_logger.error(f"Error storing model to minio: {e}")
    base_logger.info(f"Stored model_{current_time}.json to minio")

    # find the name of the latest model to make it the before pointer
    old_names_model = client.get_object(MINIO_BUCKET, "latest_models.json")
    names = old_names_model.read()
    names_json = names.decode(encoding="utf-8")
    old_names_data = json.loads(names_json)
    previous_latest = old_names_data["latest"]


    # save latest pointer to minio
    latest_model_name = {"latest": f"model_{current_time}.json",
                         "before": previous_latest}
    base_logger.info(f"latest_model_name: {latest_model_name}")
    data_name = json.dumps(latest_model_name).encode("utf-8")
    
    try:
        client.put_object(
            bucket_name=MINIO_BUCKET,
            object_name="latest_models.json",
            data=BytesIO(data_name),
            length=len(data_name),
            content_type="application/json",
            metadata={'time': current_time}
        )
    except Exception as e:
        base_logger.error(f"Error storing model to minio: {e}")
    base_logger.info(f"Stored latest models pointer to minio")

    return


async def createOccupancyModelFunction():
    base_logger.info("createOccupancyModelFunction invoked")
    base_logger.info("fetching data of past 7 days")
    data = fetch_influx_data()
    base_logger.info("calculating the average length of stay per room")
    model = find_averages_by_time_interval(data)
    base_logger.info("saving model to minio")
    save_model(model)
    base_logger.info("model trained and stored")

    return {"status": 200, "message": "Model trained and stored"}


app.deploy(createOccupancyModelFunction, "createOccupancyModelFunction-fn", "TrainOccupancyModelEvent")