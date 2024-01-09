from database_utils import *
from time import sleep
import requests

def send_data_to_kafka(topic_name, record):
    invoke_url = creds.invoke_url["batch_url"] + topic_name
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    payload = json.dumps({"records": [{"value": record}]}, cls=DateTimeEncoder)
    response = requests.post(invoke_url, headers=headers, data=payload)
    print(response.status_code)

def run_infinite_post_data_loop(connector):
    while True:
        sleep(random.randrange(0, 2))

        with connector.engine.connect() as connection:
            pin_result = fetch_row_from_db(connection, "pinterest_data")
            geo_result = fetch_row_from_db(connection, "geolocation_data")
            user_result = fetch_row_from_db(connection, "user_data")

            send_data_to_kafka("126802f17de3.pin", pin_result)
            send_data_to_kafka("126802f17de3.geo", geo_result)
            send_data_to_kafka("126802f17de3.user", user_result)

if __name__ == "__main__":
    aws_connector = AWSDBConnector()
    run_infinite_post_data_loop(aws_connector)
    print('Working')

