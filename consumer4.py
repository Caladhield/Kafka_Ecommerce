import sqlite3
from kafka import KafkaConsumer
import json

bootstrap_servers = ['localhost:9092']
topic_name = 'onlineshop'

consumer = KafkaConsumer(topic_name,
                         bootstrap_servers=bootstrap_servers,
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

conn = sqlite3.connect('winterproducts2.db')
cursor = conn.cursor()

def update_stock():
    for message in consumer:
        order = message.value
        product_id = order['product_id']
        quantity_sold = order['quantity']

        cursor.execute("SELECT quantity FROM products WHERE product_id = ?", (product_id,))
        current_stock = cursor.fetchone()[0]

        new_stock = current_stock - quantity_sold
        cursor.execute("UPDATE products SET quantity = ? WHERE product_id = ?", (new_stock, product_id))

        conn.commit()

        print(f"Updated stock for item with ID: {product_id}. New stock: {new_stock}")

if __name__ == '__main__':
    update_stock()
