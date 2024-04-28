import statistics
import pandas as pd
from joblib import load
from ProjectUtils.MessagingService.queue_definitions import (
    channel, 
    EXCHANGE_NAME, 
    ANALYTICS_TO_PROPERTY_QUEUE_ROUTING_KEY,
    property_to_analytics
)
from ProjectUtils.MessagingService.schemas import (
    from_json,
    to_json,
    MessageFactory
)


import logging

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


def calclate_difference(predictions_dict, properties_dict, median_id):
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


def calcualte_recommended_price(properties_list):
    model = load("model.joblib")
    x = pd.DataFrame(properties_list)
    predictions_dict = {} # id: price predicted
    properties_dict = {} # id: property

    for _, property in x.iterrows():
        property_df = pd.DataFrame([property]).drop("id", axis=1)
        prediction = model.predict(property_df)
        predictions_dict[property["id"]] = prediction[0]
        properties_dict[property["id"]] = property

    sorted_predictions = sorted(predictions_dict.items(), key=lambda item: item[1]) # list of tuples (id, price predicted)
    number_of_properties = len(sorted_predictions)

    if number_of_properties % 2 == 1:
        median_id = sorted_predictions[(number_of_properties // 2) + 1][0]
    else:
        median_id  = sorted_predictions[number_of_properties // 2][0]

    recommended_prices = calclate_difference(predictions_dict, properties_dict, median_id)
    
    return recommended_prices


def receive_properties(channel, method, properties, body):
    delivery_tag = method.delivery_tag

    message = from_json(body)
    print("Received message:\n" + str(message.__dict__))
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


def run():
    logger.info("Starting recommend_price service")
    channel.basic_consume(queue=property_to_analytics.method.queue, on_message_callback=receive_properties)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()


if __name__ == "__main__":
    run()