import schedule
import time
import json
import statistics
from ProjectUtils.MessagingService.queue_definitions import (
    channel, 
    EXCHANGE_NAME, 
    ANALYTICS_TO_PROPERTY_QUEUE_ROUTING_KEY,
    property_to_analytics
)
from ProjectUtils.MessagingService.schemas import (
    MessageFactory,
    to_json
)
from model import model, evaluate


def request_properties():
    message = MessageFactory.create_get_all_properties_message()
    channel.basic_publish(
        exchange=EXCHANGE_NAME,
        routing_key=ANALYTICS_TO_PROPERTY_QUEUE_ROUTING_KEY,
        body=to_json(message)
    )


def get_median(properties_list):
    prices = [property["price"] for property in properties_list]
    prediction = model.predict(prices)
    print(evaluate(prices, prediction))
    median = statistics.median(prediction)
    return median


def receive_properties(retry):
    method_frame, _, body = channel.basic_get(queue=property_to_analytics, auto_ack=True)
    if method_frame:
        properties_list = json.loads(body)
        get_median(properties_list)
    elif retry > 0:
        print("No properties received. Retrying...")
        time.sleep(1) 
        receive_properties()
        retry -= 1
    else:
        print("No properties received")


schedule.every().day.at("00:00").do(request_properties)
schedule.every().day.at("00:01").do(receive_properties, retry=3)

while True:
    schedule.run_pending()
    time.sleep(1)