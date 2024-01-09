import json
import random
from multiprocessing import Process
from time import sleep
import uuid
from datetime import datetime

import requests
import sqlalchemy
from sqlalchemy import text

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class AWSDBConnector:
    def __init__(self):
        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        self.engine = self.create_db_connector()

    def create_db_connector(self):
        return sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")


def fetch_row_from_db(connection, table_name):
    random_row = random.randint(0, 11000)
    query = text(f"SELECT * FROM {table_name} LIMIT {random_row}, 1")
    selected_row = connection.execute(query)
    for row in selected_row:
        return dict(row._mapping)


def send_data_to_kinesis(stream_name, records):
    invoke_url = f"https://nv2gj4px8k.execute-api.us-east-1.amazonaws.com/test-streaming/streams/{stream_name}/record"
    headers = {'Content-Type': 'application/json'}
    payload = json.dumps({
        "StreamName": stream_name,
        "Data": records,
        "PartitionKey": str(uuid.uuid4())
    }, cls=DateTimeEncoder)
    response = requests.request("PUT", invoke_url, headers=headers, data=payload)
    print(response.status_code)


def run_infinite_post_data_loop(connector):
    while True:
        sleep(random.randrange(0, 2))

        with connector.engine.connect() as connection:
            pin_result = fetch_row_from_db(connection, "pinterest_data")
            geo_result = fetch_row_from_db(connection, "geolocation_data")
            user_result = fetch_row_from_db(connection, "user_data")

            send_data_to_kinesis("streaming-126802f17de3-pin", pin_result)
            send_data_to_kinesis("streaming-126802f17de3-geo", geo_result)
            send_data_to_kinesis("streaming-126802f17de3-user", user_result)


if __name__ == "__main__":
    aws_connector = AWSDBConnector()
    run_infinite_post_data_loop(aws_connector)
    print('Working')

