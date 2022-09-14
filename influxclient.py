import logging
from urllib3 import Retry
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS, WritePrecision
import datetime

retries = Retry(connect=5, read=2, redirect=5)
logger = logging.getLogger()

class ic():
    def __init__(self, INFLUX_URL, TOKEN, ORG, BUCKET):
        self.client = InfluxDBClient(url=INFLUX_URL, token=TOKEN, org=ORG, retries=retries)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.client.query_api()
        self.BUCKET = BUCKET
        self.ORG = ORG

    def normalise_timetag(self, timetag):
        return timetag.replace(" ", "_")


    def get_timetag_data(self, timetag):
        #influx_timetag = self.normalise_timetag(timetag)
        query = f'from(bucket:"{self.BUCKET}")\
        |> range(start: -48h) \
        |> filter(fn: (r) => r.timetag == "{timetag}") '
        result = self.query_api.query(org=self.ORG, query=query)
        return result

    def timetag_to_time(self, timetag):
        # 14_September_2022
        dd =datetime.datetime.strptime(timetag, "%d %B %Y")
        print(dd.strftime('%Y-%m-%dT%H:%M:%S'))
        return dd.strftime('%Y-%m-%dT%H:%M:%S')

    def put_data_in_bucket(self, data_dict, point_name, timetag=None):
        for item in data_dict:
            city = item.pop('city')
            points = []
            for measurement, value in item.items():
                p = Point(point_name).tag("city", city).field(measurement, value).time("2022-09-10T11:12:22.473Z")
                if timetag:
                    n_timetag = self.normalise_timetag(timetag)
                    p.tag("timetag", n_timetag)
                    p.time(self.timetag_to_time(timetag))
                points.append(p)
                self.write_api.write(bucket=self.BUCKET, record=points, write_precision=WritePrecision.S)
        return 1


    def put_data_in_bucket_wrapper(self, data_dict, timetag, point_name):
        if timetag:
            n_timetag = self.normalise_timetag(timetag)
            if not (self.get_timetag_data(n_timetag)):
                return self.put_data_in_bucket(data_dict, point_name, timetag)
            else:
                return 0
