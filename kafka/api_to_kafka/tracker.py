import json
from confluent_kafka import Consumer

consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "weather_data_tracker",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_config)
consumer.subscribe(["weather_data_demo"])

print("🟢 Consumer is running and subscribed to 'weather_data_demo'")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"❌ Error: {msg.error()}")
            continue
        
        # 1. Decode bytes and parse JSON
        value = msg.value().decode("utf-8")
        weather = json.loads(value)
        
        # 2. Extract Open-Meteo specific fields
        # Note: Open-Meteo structure is usually {'current': {'temperature_2m': ...}}
        current_temp = weather['current']['temperature_2m']
        latitude = weather['latitude']
        longitude = weather['longitude']

        print(f"🌡️ Weather Update: {current_temp}°C at Coord [{latitude}, {longitude}]")

except KeyboardInterrupt:
    print("\n🔴 Stopping consumer")
finally:
    consumer.close()
