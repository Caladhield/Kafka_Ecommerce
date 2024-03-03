from kafka import KafkaConsumer
import json
from datetime import datetime, timezone

bootstrap_servers = ['localhost:9092']
topic_name = 'onlineshop'

consumer = KafkaConsumer(topic_name,
                         bootstrap_servers=bootstrap_servers,
                         auto_offset_reset='earliest',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

def count_orders():
    order_count = 0
    daystart = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    
    for message in consumer:
        order = message.value
        order_timestamp = order.get('timestamp', 0)

        if order_timestamp >= daystart:
            order_count += 1
        else:
            continue

        print(f"Order received. Order ID: {order['order_id']}, Product: {order['product_name']}, Quantity: {order['quantity']}, Orders Today: {order_count}")

        current_time = datetime.now().timestamp()
        if current_time >= (daystart + 86400):
            order_count = 0
            daystart = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()

if __name__ == '__main__':
    count_orders()
