import pandas as pd
import requests
import json
import logging
from ProjectUtils.MessagingService.queue_definitions import (
    channel, 
    EXCHANGE_NAME, 
    ANALYTICS_TO_PROPERTY_QUEUE_ROUTING_KEY,
    property_to_analytics,
    property_to_analytics_data
)
from ProjectUtils.MessagingService.schemas import (
    from_json,
    to_json,
    MessageFactory
)
from model_rec import calcualte_recommended_price_by_model
from real_price_rec import calculate_real_price_difference


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


def calcualte_recommended_price(properties_list):

    real_prices_df = pd.DataFrame(properties_list)[["price", "id", "location"]]
    X = pd.DataFrame(properties_list).drop(["price","location"], axis = 1)

    recommended_price_by_model = calcualte_recommended_price_by_model(X)

    recommended_prices_real_price = calculate_real_price_difference(real_prices_df)

    #recommended_prices_trends = recommend_price_by_trends(real_prices_df)

    recommended_prices = {}

    for property_id in recommended_price_by_model.keys():
        recommended_prices[property_id] = (recommended_price_by_model[property_id] + recommended_prices_real_price[property_id]) / 2

    return recommended_prices    


def receive_properties(channel, method, properties, body):
    delivery_tag = method.delivery_tag

    message = from_json(body)
    logger.info("Received message:\n" + str(message.__dict__))

    properties_list = message.body
    recommended_prices = calcualte_recommended_price(properties_list)
    response_message = MessageFactory.create_recommended_price_response_message(recommended_prices)
    channel.basic_publish(
        exchange=EXCHANGE_NAME,
        routing_key=ANALYTICS_TO_PROPERTY_QUEUE_ROUTING_KEY,
        body=to_json(response_message)
    )

    channel.basic_ack(delivery_tag)


def send_data_to_elasticsearch(channel, method, properties, body):
        
    delivery_tag = method.delivery_tag

    message = from_json(body)

    properties = message.body

    logger.info("Sending data to Elasticsearch")
    for p in properties:
        url = "http://elasticsearch:9200/property/_doc/" + p["id"]
        data = json.dumps(p)
        response = requests.post(url, data=data, headers={'Content-Type': 'application/json'})
        if response.status_code >= 300 or response.status_code < 200:
            logger.error(f"Error sending data to Elasticsearch: {response.status_code} - {response.text}")
            

    channel.basic_ack(delivery_tag)


def run():
    logger.info("Starting analytics service")
    channel.basic_consume(queue=property_to_analytics.method.queue, on_message_callback=receive_properties)
    channel.basic_consume(queue=property_to_analytics_data.method.queue, on_message_callback=send_data_to_elasticsearch)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()



if __name__ == "__main__":
    run()

