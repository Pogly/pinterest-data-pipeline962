from AWSDBConnector import AWSDBConnector
from sqlalchemy import text
from time import sleep
from multiprocessing import Process

import requests
import random
import boto3
import json
import sqlalchemy
import yaml

random.seed(100)

# This code opens the file named "TopicsKey.Yaml" in the current directory and loads the content
#  of the YAML file in the variable `invoke_url`, This should be the path to a Kafka Server.
with open("TopicsKey.Yaml") as Endpoint:
    invoke_url = yaml.safe_load(Endpoint)

# NOTE remember your typing of parameters and object returned from the method/function
def send_to_batch_endpoint(Topic: str, payload: dict) -> None:
    """
    The function `send_To_Kafka` sends a payload to an EC2 instance using a POST request with specified
    headers and topic.
    
    :param Topic:The topic to send the data in the Kafka Server
    :param payload:The Data you want to send. This should be in a JSON format
    """
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    # NOTE would check the status code here is returning 200 and deal with the exception if not.
    response = requests.request("POST", f"{invoke_url}{Topic}", headers=headers, data=payload)
    if response.status_code == 200:
        print("sending to Kafka Server!")
    else:
        print("Oh no")



new_connector = AWSDBConnector()

def run_infinite_post_data_loop():
    """
    The `run_infinite_post_data_loop` function continuously retrieves random rows from different tables,
    formats the data into JSON payloads, and sends them to specific Kafka topics.
    """
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:
            # NOTE would create that pulls the row from the table 
            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

                # The `payload_pin` variable is being created as a JSON string using the
                # `json.dumps()` function. It is formatting the data in a specific structure before
                # sending it to an EC2 instance.
                # NOTE I would add all of the payloads to their own module and import. Maybe a module called payload_schemas.py?
                payload_pin = json.dumps({
                "records": [
                {    
                 "value": {"index": pin_result["index"], "unique_id": pin_result["unique_id"], "title": pin_result["title"], "description": pin_result["description"]
                           , "poster_name": pin_result["poster_name"], "follower_count": pin_result["follower_count"], "tag_list": pin_result["tag_list"], "is_image_or_video": pin_result["is_image_or_video"]
                           , "image_src": pin_result["image_src"], "downloaded": pin_result["downloaded"], "save_location": pin_result["save_location"], "category": pin_result["category"]}
                }]})
                send_to_batch_endpoint("e89446818119.pin",payload_pin)



            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

                # The `payload_geo` variable is being created as a JSON string using the
                # `json.dumps()` function. It is formatting the data in a specific structure before
                # sending it to an EC2 instance.
                # NOTE would write the dictionary entries vertically like below
                payload_geo = json.dumps({
                "records": [
                    { 
                        "value": {
                            "ind": geo_result["ind"], 
                            "timestamp": geo_result["timestamp"], 
                            "latitude": geo_result["latitude"], 
                            "longitude": geo_result["longitude"], 
                            "country": geo_result["country"]
                        }
                    }
                ]}, default=str)


                send_to_batch_endpoint("e89446818119.geo",payload_geo)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
                
                # The `payload_user` variable is being created as a JSON string using the
                # `json.dumps()` function. It is formatting the data in a specific structure before
                # sending it to an EC2 instance.
                payload_user = json.dumps({
                "records": [
                {    
                 "value": {"ind": user_result["ind"], "first_name": user_result["first_name"], "last_name": user_result["last_name"], "age": user_result["age"], "date_joined": user_result["date_joined"]}
                }]}, default=str)

                send_to_batch_endpoint("e89446818119.user",payload_user)
            
            print(pin_result)
            print(geo_result)
            print(user_result)


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    