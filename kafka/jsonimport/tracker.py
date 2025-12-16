# this microservice tracks new events
# we are going to connect to kafka brocker so that we can subscribe to a topic, then start listening to messages in that topic, once a message gets in..we process it one by one

import json
from confluent_kafka import Consumer

# group.id : A unique string that identifies the consumer group this consumer belong to.
# "auto.offset.rest" : tells a kafka consumer what to do if it cannot find where it last left of reading messages.

consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id" : "order-tracker",
    "auto.offset.reset" : "earliest"
}

consumer = Consumer(consumer_config)

# consumer is subscribing to topics inorder to read the events
# one consumer can subscribe to multiple topics hence an array here ["orders"]

consumer.subscribe(["orders"])

print("🟢 Consumer is running and subscribed to orders topic")

try:
    while True:
        # here a continous loop is ran to check for new events
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("❌ Error:", msg.error())
            continue
        
        # when there is a new event we decode it and then we transform it into string
        value = msg.value().decode("utf-8")
        # and we transform string into python dictonary
        order = json.loads(value)
        print(f"📦 Received order: {order['quantity']} x {order['item']} from {order['user']}")
except KeyboardInterrupt:
    print("\n🔴 Stopping consumer")

finally:
    consumer.close()
