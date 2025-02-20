from AWSDBConnector import AWSDBConnector
from multiprocessing import Process
from sqlalchemy import text
from time import sleep

import boto3
import json
import requests
import random
import sqlalchemy
import yaml

random.seed(100)

with open("APIKey.Yaml") as Endpoint:
    invoke_url = yaml.safe_load(Endpoint)

def send_To_stream(payload):
    """
    The function `send_To_stream` sends a payload to a Kinesis stream using a PUT request with JSON data.
    
    :param payload: The Data you want to send. This should be in a JSON format. This payload should be structured 
    with specific keys such as "StreamName", "Data", and "PartitionKey" to be sent to a Kinesis stream
    """


    headers = {'Content-Type': 'application/json'}
    response = requests.request("PUT",f"{invoke_url}", headers=headers, data=payload)

new_connector = AWSDBConnector()


def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

                
                payload_pin = json.dumps({
                "StreamName": "Kinesis-Prod-Stream",
                "Data": {
                        "index": pin_result["index"], "unique_id": pin_result["unique_id"], "title": pin_result["title"], "description": pin_result["description"]
                           , "poster_name": pin_result["poster_name"], "follower_count": pin_result["follower_count"], "tag_list": pin_result["tag_list"], "is_image_or_video": pin_result["is_image_or_video"]
                           , "image_src": pin_result["image_src"], "downloaded": pin_result["downloaded"], "save_location": pin_result["save_location"], "category": pin_result["category"]
                           },
                "PartitionKey": "pin-Data"
                }, default=str)
                send_To_stream(payload_pin,)


            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

                payload_geo = json.dumps({
                 "StreamName": "Kinesis-Prod-Stream",
                "Data": {    
                        "ind": geo_result["ind"], "timestamp": geo_result["timestamp"], "latitude": geo_result["latitude"], "longitude": geo_result["longitude"], "country": geo_result["country"]
                           },
                "PartitionKey": "geo-Data"
                }, default=str)

                send_To_stream(payload_geo)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)

                payload_user = json.dumps({
                 "StreamName": "Kinesis-Prod-Stream",
                "Data": {
                        "ind": user_result["ind"], "first_name": user_result["first_name"], "last_name": user_result["last_name"], "age": user_result["age"], "date_joined": user_result["date_joined"]
                           },
                "PartitionKey": "user-Data"
                }, default=str)

                send_To_stream(payload_user)


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    