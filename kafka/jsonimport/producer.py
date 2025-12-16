import json
import uuid
from confluent_kafka import Producer

# here we are producing new kafka Producer and we are telling it where kafka is accessible which is 'localhost:9092'
# KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
# Producer kafka client can talk to kafka server which is on localhost
# 'bootstrap.servers' : Provides the initial host that acts as the starting point for a Kafka client to discover the full set of alive servers in the cluster.

producer_config = {
    "bootstrap.servers": "localhost:9092"
}
producer = Producer(producer_config)

# a callback to track whether our message was delivered successfully or not.
def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered {msg.value().decode("utf-8")}")
        print(f"✅ Delivered to {msg.topic()} : partition {msg.partition()} : at offset {msg.offset()}")


# next we create and prepare an event that we're going to send to Kafka as a producer
# uuid is library to generate random 128-bit numbers designed to be globally unique and commonly used for identifiers.
# kafka relies on unique identifiers internally for things like cluster IDs, topic IDs etc.

order = {
    "order_id": str(uuid.uuid4()),
    "user": "lara",
    "item": "frozen yogurt",
    "quantity": 10
}

# now 'order' is a json object, and now we need to convert JSON into kafka compatible format which is bytes
# object datatype is dictonary {"order_id": str(uuid.uuid4()),"user": "lara","item": "frozen yogurt","quantity": 10}

# First we convert this dictonary datatype or object datatype to a JSON string
# json.dumps converts it into JSON string
# .encode("utf-8") converts the json string into bytes format which kafka actually understands

value = json.dumps(order).encode("utf-8")

# now we have 'value' which is ready to be sent into kafka
# producer holds the connection to kafka
# this sends 'value' to kafka and tells it to save it or add this into topic called 'orders'
# if the topic="orders" doesn't exist yet, kafka will create a new topic and save value there.
# Every time the value will get appended to under the respective topic

producer.produce(
    topic="orders",
    value=value,
    callback=delivery_report
)

# this means kafka producer buffers producer for perfomance
# basically send data in batch
# if program crashed or stops at some point, the producer.flush() will make sure that all these buffered events that haven't been sent yet get sent.

producer.flush()




