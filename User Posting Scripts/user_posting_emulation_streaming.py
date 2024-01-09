from database_utils import *
from time import sleep
import requests
import uuid

def send_data_to_kinesis(stream_name, records):
    invoke_url = creds.invoke_url["stream_url"] + stream_name + "/record"
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

