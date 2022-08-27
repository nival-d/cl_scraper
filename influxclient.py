import logging
from urllib3 import Retry
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS, WritePrecision

retries = Retry(connect=5, read=2, redirect=5)
logger = logging.getLogger()

class ic():
    def __init__(self, INFLUX_URL, TOKEN, ORG, BUCKET):
        self.client = InfluxDBClient(url=INFLUX_URL, token=TOKEN, org=ORG, retries=retries)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.client.query_api()
        self.BUCKET = BUCKET

    def put_data_in_bucket(self, data_dict, point_name):
        for item in data_dict:
            city = item.pop('city')
            points = []
            for measurement, value in item.items():
                points.append(Point(point_name).tag("city", city).field(measurement, value))
                self.write_api.write(bucket=self.BUCKET, record=points, write_precision=WritePrecision.S)
