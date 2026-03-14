import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# Teaching note: These lists make it easy to explain 'variety' and how categorical fields support group-by analytics.
COUNTRIES = ['Canada', 'USA', 'UK', 'Germany', 'UAE', 'Pakistan']
PRODUCTS = ['P-0001', 'P-0002', 'P-0003', 'P-0042', 'P-0100']

def make_event(i: int) -> dict:
    """Create one simulated retail event."""
    return {
        'order_id': f'ORD-{100000 + i}',
        'product_id': random.choice(PRODUCTS),
        'country': random.choice(COUNTRIES),
        'price': round(random.uniform(5, 120), 2),
        'quantity': random.randint(1, 4),
        # ISO timestamp makes parsing predictable in Spark.
        'event_time': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
    }

def main():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print('Producer started. Sending events to topic: retail-events')
    i = 0
    while True:
        event = make_event(i)
        producer.send('retail-events', value=event)
        print('Sent:', event)
        i += 1
        time.sleep(1)  # 1 event/second

if __name__ == '__main__':
    main()
