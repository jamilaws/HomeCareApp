import datetime
import os
from datetime import timedelta
import pandas as pd

from influxdb_client import InfluxDBClient
from minio import Minio
from io import BytesIO
from base import LocalGateway, base_logger


BUCKET_KITCHEN = "1_2_9"
BUCKET_BATHROOM = "1_3_6"
BUCKET_ROOM = "1_2_2"
BUCKET_DOOR = "1_4_8"
BUCKETS_PIR = [BUCKET_KITCHEN, BUCKET_BATHROOM, BUCKET_ROOM]


INFLUX_ORG = "wise2024"
INFLUX_TOKEN = os.environ.get("INFLUXDB_HOST", None)
INFLUX_USER = os.environ.get("INFLUXDB_USER", None)
INFLUX_PASS = os.environ.get("INFLUXDB_PASS", None)
SIF_SCHEDULER = os.environ.get("SCH_SERVICE_NAME", None)

MINIO_SERVER = "192.168.178.63:9091"
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", None)
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", None)
MINIO_BUCKET = "models"

if INFLUX_TOKEN is None or INFLUX_USER is None or INFLUX_PASS is None or SIF_SCHEDULER is None:
    raise ValueError("Missing env variables")


def fetch_data(bucket, measurement):
    with InfluxDBClient(url=INFLUX_TOKEN, org=INFLUX_ORG, username=INFLUX_USER, password=INFLUX_PASS, verify_ssl=False) as client:
        p = {
            "_start": timedelta(days=-7),
        }

        query_api = client.query_api()
        tables = query_api.query(f'''
                                 from(bucket: "{bucket}") |> range(start: _start)
                                |> filter(fn: (r) => r["_measurement"] == "{measurement}")
                                 ''', params=p)
        obj = []
        base_logger.info(tables)
        for table in tables:
            for record in table.records:
                val = {}
                base_logger.info(record)
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
    durations = data.groupby(["group", "bucket"]).agg(
        start=("timestamp", "min"),
        end=("timestamp", "max"))
    durations["duration"] = (durations["end"] - durations["start"]).dt.total_seconds() / 60  # Dauer in Minuten
    average_duration = durations.groupby("bucket")["duration"].mean().reset_index()
    average_duration.columns = ["bucket", "average_duration_minutes"]

    return average_duration


def save_model(model):
    with Minio(MINIO_SERVER, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY) as client:

        found = client.bucket_exists(MINIO_BUCKET)
        if not found:
            client.make_bucket(MINIO_BUCKET)

        current_time = datetime.datetime.now().strftime("%d-%m-%y_%H-%M-%S")
        data = pd.DataFrame.to_json(model).encode("utf-8")
        client.put_object(
            bucket_name=MINIO_BUCKET,
            object_name=f"model_{current_time}.json",
            data=BytesIO(data),
            length=len(data),
            content_type="application/json",
            metadata={'time': current_time}
        )

        base_logger.info(f"Stored model_{current_time}.json to minio")

    return


async def createOccupancyModelFunction():
    base_logger.info("fetching data of past 7 days")
    data = fetch_influx_data()
    base_logger.info("calculating the average length of stay per room")
    model = find_averages(data)
    base_logger.info("saving model to minio")
    save_model(model)
    base_logger.info("model trained and stored")

    return {"status": 200}


app = LocalGateway()
app.deploy(createOccupancyModelFunction, "createOccupancyModelFunction-fn", "TrainOccupancyModelEvent")

