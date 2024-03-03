import sqlite3
import random
import json
from kafka import KafkaProducer
from datetime import datetime, timezone
import time

bootstrap_servers = ['localhost:9092']
topic_name = 'onlineshop'

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

conn = sqlite3.connect('winterproducts.db')
cursor = conn.cursor()
cursor.execute("SELECT product_id, product_name, sale_price_USD FROM products")
products = [{'product_id': row[0], 'name': row[1], 'price': row[2]} for row in cursor.fetchall()]
conn.close()

def generate_order():
    product = random.choice(products)
    order = {
        'order_id': random.randint(10000, 99999),
        'product_id': product['product_id'],
        'product_name': product['name'],
        'quantity': random.randint(1, 5),
        'sale_price_USD': product['price'],
        'timestamp': int(time.time())
    }
    return order

def simulate_orders():
    print("Starting orders. Ctrl+C to stop.")
    try:
        while True:
            order = generate_order()
            producer.send(topic_name, value=order)
            print(f" Sent order: {json.dumps(order, indent=4)}")
            time.sleep(1.0) 
    except KeyboardInterrupt:
        print("Orders stopped.")

if __name__ == '__main__':
    simulate_orders()

