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


def get_median(properties_list):
    model = load("model.joblib")
    x = pd.DataFrame(properties_list)
    predictions_list = []
    for _, property in x.iterrows():
        property_df = pd.DataFrame([property])
        prediction = model.predict(property_df)
        predictions_list.append(prediction[0])
    print(predictions_list)
    median = statistics.median(predictions_list)
    return median


def receive_properties(channel, method, properties, body):
    delivery_tag = method.delivery_tag

    message = from_json(body)
    print("Received message:\n" + str(message.__dict__))

    properties_list = message.body
    median = get_median(properties_list)
    response_message  = MessageFactory.create_recommended_price_response_message({"median": median})
    channel.basic_publish(
        exchange=EXCHANGE_NAME,
        routing_key=ANALYTICS_TO_PROPERTY_QUEUE_ROUTING_KEY,
        body=to_json(response_message)
    )

    channel.basic_ack(delivery_tag)


def run():
    channel.basic_consume(queue=property_to_analytics.method.queue, on_message_callback=receive_properties)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()


if __name__ == "__main__":
    run()