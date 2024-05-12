import statistics
import pandas as pd
import numpy as np
import requests
import json
import logging
from joblib import load
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


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

features_comparison = {
    "bathrooms": 0.03,
    "bedrooms": 0.05,
    "beds": 0.08,
    "number_of_guests": 0.07,
    "num_amenities": 0.02
}

old_prices_df = pd.DataFrame()

def calclate_features_difference(predictions_dict, properties_dict, median_id):
    recommended_prices = {} # id : recommended price

    for property_id in predictions_dict.keys():
        if property_id == median_id:
            recommended_prices[property_id] = predictions_dict[property_id]
        else:
            property = properties_dict[property_id]
            median_property = properties_dict[median_id]
            median_price = predictions_dict[median_id]
            total_difference = 0

            for feature in features_comparison.keys():
                feature_diff = property[feature] - median_property[feature]
                total_difference += feature_diff * features_comparison[feature]

            recommended_prices[property_id] = median_price * (1+total_difference)

    return recommended_prices


def calcualte_recommended_price_by_model(X):
    model = load("model.joblib")
    
    predictions_dict = {} # id: price predicted
    properties_dict = {} # id: property

    for _, property in X.iterrows():
        property_df = pd.DataFrame([property]).drop("id", axis=1)
        prediction = model.predict(property_df)
        predictions_dict[property["id"]] = prediction[0]
        properties_dict[property["id"]] = property

    median_price = statistics.median_high(predictions_dict.values())
    median_id = next(key for key, value in predictions_dict.items() if value == median_price)

    recommended_prices = calclate_features_difference(predictions_dict, properties_dict, median_id)
    
    return recommended_prices


def calculate_real_price_difference_by_location(real_prices_df, old_prices_df):
    
    old_prices = old_prices_df["price"].astype(np.float32, copy = False)
    prices = real_prices_df["price"].astype(np.float32, copy = False)
    old_mean = statistics.mean(old_prices)
    new_mean = statistics.mean(prices)
    general_porcentage_difference = (new_mean - old_mean) / old_mean

    recommended_prices = {}

    for _, property in real_prices_df.iterrows():
        old_price = old_prices_df[old_prices_df["id"] == property["id"]]["price"].iloc[0]
        price_change = property["price"] - old_price
        if price_change == 0 and abs(general_porcentage_difference) >= 0.10:
            recommended_prices[property["id"]] = property["price"] * (1 + general_porcentage_difference)
        else:
            recommended_prices[property["id"]] = property["price"]

    old_prices_df = real_prices_df
    return recommended_prices


def calculate_real_price_difference(real_prices_df):

    global old_prices_df
    if old_prices_df.empty:
        old_prices_df = real_prices_df
        return old_prices_df.set_index('id')['price'].to_dict()

    recommended_prices_real_price = {}
    locations = real_prices_df["location"].unique()

    for location in locations:
        real_prices_by_location_df = real_prices_df[real_prices_df["location"] == location]
        old_prices_by_location_df = old_prices_df[old_prices_df["location"] == location]
        recommended_prices_by_location = calculate_real_price_difference_by_location(real_prices_by_location_df, old_prices_by_location_df)
        recommended_prices_real_price = recommended_prices_real_price | recommended_prices_by_location

    return recommended_prices_real_price


def calcualte_recommended_price(properties_list):

    real_prices_df = pd.DataFrame(properties_list)[["price", "id", "location"]]
    X = pd.DataFrame(properties_list).drop(["price","location"], axis = 1)

    recommended_price_by_model = calcualte_recommended_price_by_model(X)

    recommended_prices_real_price = calculate_real_price_difference(real_prices_df)

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

