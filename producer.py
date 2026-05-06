# producer.py
import json
import time
import requests
from kafka import KafkaProducer
import env

def create_producer():
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def fetch_and_publish():
    producer = create_producer()
    api_url = "https://api.openaq.org/v3/parameters/2/latest"
    
    # We poll a few specific locations to simulate our stream
    params = {"limit": 50, "parameter": "pm25"} 

    print("Starting OpenAQ ingestion stream...")
    while True:
        try:
            headers = {"X-API-Key": env.API_KEY}
            response = requests.get(api_url, params=params, headers=headers)
            data = response.json()

            print("data", data)
            
            for item in data.get('results', []):
                # 1. Format the new nested timestamp for Flink 
                # (Converts "2026-05-05T13:00:00Z" to "2026-05-05 13:00:00")
                raw_time = item.get('datetime', {}).get('utc', '')
                flink_time = raw_time.replace('T', ' ').replace('Z', '') if raw_time else ''

                # 2. Map the new v3 keys (locationsId) to your Flink Schema
                payload = {
                    "location": f"Station-{item.get('locationsId', 'Unknown')}",
                    "city": "Global-Network", # v3 doesn't pass city directly here
                    "parameter": "pm25",
                    "value": item.get('value', 0.0),
                    "timestamp": flink_time
                }
                
                # 3. Send to Kafka
                producer.send('raw-air-quality', value=payload)
                print(f"Sent: {payload['location']} -> {payload['value']} PM2.5")
            
            producer.flush()
            time.sleep(10) # Poll every 10 seconds to simulate live stream
            
        except Exception as e:
            print(f"Connection error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    fetch_and_publish()