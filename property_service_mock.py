import schedule
import time
from pydantic import BaseModel
from ProjectUtils.MessagingService.queue_definitions import (
    channel, 
    EXCHANGE_NAME, 
    PROPERTY_TO_ANALYTICS_QUEUE_ROUTING_KEY,
    analytics_to_property
)
from ProjectUtils.MessagingService.schemas import (
    MessageFactory,
    to_json, 
    from_json
)

"""
[{
    "id": "661a7e5ab7bd0512178cf014",
    "latitude": 40.639337,
    "longitude": -8.65099,
    "bathrooms": 2,
    "bedrooms": 3,
    "beds": 4,
    "number_of_guests": 4,
    "num_amenities": 4
},
...
]
"""

# needs to be in this order, given train data was in this order
class PropertyForAnalytics(BaseModel):
    id: str
    latitude: float
    longitude: float
    bathrooms: int
    bedrooms: int
    beds: int
    number_of_guests: int
    num_amenities: int


properties = [
    PropertyForAnalytics(
        id="661a7e5ab7bd0512178cf014",
        latitude=40.639337,
        longitude=-8.65099,
        bathrooms=2,
        bedrooms=3,
        beds=4,
        number_of_guests=4,
        num_amenities=4
    ),
    PropertyForAnalytics(
        id="1f994177dc45954c6148086",
        latitude=40.649127,
        longitude=-8.65455,
        bathrooms=1,
        bedrooms=1,
        beds=1,
        number_of_guests=2,
        num_amenities=3
    ),
    PropertyForAnalytics(
        id="60a283db136301428a0ae06",
        latitude=40.63892,
        longitude=-8.65459,
        bathrooms=1,
        bedrooms=1,
        beds=2,
        number_of_guests=2,
        num_amenities=2
    ),
    PropertyForAnalytics(
        id="1ac9af963e829344c53956b",
        latitude=40.64131,
        longitude=-8.65431,
        bathrooms=2,
        bedrooms=2,
        beds=3,
        number_of_guests=4,
        num_amenities=5
    ),
    PropertyForAnalytics(
        id="2f224177dc45954c6148086",
        latitude=40.64243,
        longitude=-8.64571,
        bathrooms=2,
        bedrooms=2,
        beds=3,
        number_of_guests=4,
        num_amenities=6
    ),
    PropertyForAnalytics(
        id="1e8b214c28d15241902904a",
        latitude=40.63889,
        longitude=-8.64554,
        bathrooms=2,
        bedrooms=3,
        beds=5,
        number_of_guests=6,
        num_amenities=7
    ),
    PropertyForAnalytics(
        id="f91622341c0651489309cfa",
        latitude=40.63848,
        longitude=-8.65434,
        bathrooms=3,
        bedrooms=3,
        beds=6,
        number_of_guests=6,
        num_amenities=8
    ),
    PropertyForAnalytics(
        id="87e1dfd41f80d24d1e1845d",
        latitude=40.63956,
        longitude=-8.65434,
        bathrooms=3,
        bedrooms=4,
        beds=3,
        number_of_guests=4,
        num_amenities=3
    ),
]

def request_recommended_price():
    json_properties = [property.model_dump() for property in properties]
    message = MessageFactory.create_get_recommended_price(json_properties)
    channel.basic_publish(
        exchange=EXCHANGE_NAME,
        routing_key=PROPERTY_TO_ANALYTICS_QUEUE_ROUTING_KEY,
        body=to_json(message)
    )
    print("Mock property service: Request message sent!")
    time.sleep(5)
    while True:
        method_frame, _, body = channel.basic_get(queue=analytics_to_property.method.queue, auto_ack=True)
        if method_frame:
            message = from_json(body)
            print("Received message:\n" + str(message.__dict__))
            print("Recommended prices for each property: " + str(message.body))
            break
    
request_recommended_price()    
"""
schedule.every().day.at("00:00:00").do(request_recommended_price)

while True:
    schedule.run_pending()
    time.sleep(1)
"""