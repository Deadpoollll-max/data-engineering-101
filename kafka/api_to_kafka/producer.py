import json
import time
import requests
from quixstreams import Application

# Correct delivery callback for Quix Streams 2025
def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Kafka Error: {err}")
    else:
        print(f"✅ Produced to {msg.topic()} [partition {msg.partition()}] at offset {msg.offset()}")

def fetch_and_produce():
    # Initialize Quix Application
    app = Application(
        broker_address="localhost:9092", 
        loglevel="INFO"
    )

    # API Configuration
    # Ensure current is a single string of comma-separated variables
    api_url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 51.5,
        "longitude": -0.11,
        "current": "temperature_2m", # Open-Meteo expects a string here
        "timezone": "auto"
    }

    print("🚀 Weather Streamer started. Monitoring London...")

    # Open producer context
    with app.get_producer() as producer:
        try:
            while True:
                try:
                    # Fetching from API
                    response = requests.get(api_url, params=params, timeout=10)
                    response.raise_for_status() # This catches the 400 error
                    
                    weather_data = response.json()
                    
                    # Manual serialization for reliability
                    payload = json.dumps(weather_data).encode("utf-8")

                    # Produce to Kafka
                    producer.produce(
                        topic="weather_data_demo",
                        key="London",
                        value=payload,
                        on_delivery=delivery_report # 'on_delivery' is the correct 2025 keyword
                    )
                    
                    producer.flush()
                    temp = weather_data['current']['temperature_2m']
                    print(f"📡 API Success: Current London Temp is {temp}°C")

                except requests.exceptions.HTTPError as e:
                    print(f"⚠️ API Error (HTTP {e.response.status_code}): {e.response.text}")
                except Exception as e:
                    print(f"⚠️ General Error: {e}")
                
                # Poll every 60 seconds
                time.sleep(60)

        except KeyboardInterrupt:
            print("\n🛑 Streamer stopped by user.")

if __name__ == "__main__":
    fetch_and_produce()
