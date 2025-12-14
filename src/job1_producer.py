import requests
import json
import time
from kafka import KafkaProducer
from datetime import datetime


def fetch_articles_from_api():
    url = 'https://eventregistry.org/api/v1/article/getArticles'
    
    request_payload = {
        'apiKey': '47805c83-4803-4c3f-863e-0133747c8681',
        'keyword': 'technology',
        'articlesCount': 5,
    }

    try:
        response = requests.get(url, json=request_payload, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching data from API: {e}")
        return None


def send_to_kafka(producer, topic, data):
    try:
        message = {
            'timestamp': datetime.now().isoformat(),
            'data': data
        }

        producer.send(topic, value=message)
        
        print(f"Message sent to topic {topic}")
        return True
    except Exception as e:
        print(f"Error sending message to Kafka: {e}")
        return False


def run_producer(kafka_bootstrap_servers='localhost:9092', topic='raw_events', interval=30, duration=3600):
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        max_block_ms=5000
    )
    
    print(f"Starting producer... Will fetch data every {interval} seconds for {duration} seconds")
    
    start_time = time.time()
    message_count = 0
    
    try:
        while (time.time() - start_time) < duration:
            print(f"Fetching data from API (message #{message_count + 1})...")
            data = fetch_articles_from_api()
            
            if data:
                if send_to_kafka(producer, topic, data):
                    message_count += 1
                    print(f"Successfully sent message #{message_count}")

            time.sleep(interval)
    finally:
        producer.close()
        print(f"\nProducer finished. Total messages sent: {message_count}")


if __name__ == '__main__':
    run_producer(
        kafka_bootstrap_servers='localhost:9092',
        topic='raw_events',
        interval=30,
        duration=3600
    )
