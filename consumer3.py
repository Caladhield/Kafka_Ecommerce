import json
from kafka import KafkaConsumer
from datetime import datetime, timezone, timedelta
import schedule
import time

bootstrap_servers = ['localhost:9092']
topic_name = 'onlineshop'

consumer = KafkaConsumer(topic_name,
                         bootstrap_servers=bootstrap_servers,
                         auto_offset_reset='earliest',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                         consumer_timeout_ms=10000)

def create_daily_report():
    daily_orders = 0
    daily_sales = 0.0
    product_sales = {}

    today = datetime.now(timezone.utc).date()

    for message in consumer:
        order = message.value
        order_timestamp = datetime.fromtimestamp(order['timestamp'], tz=timezone.utc).date()

        if order_timestamp == today:
            daily_orders += 1

            sale_price_usd = order.get('sale_price_USD', 0)
            order_total = order['quantity'] * sale_price_usd
            daily_sales += order_total

            product_id = order['product_id']
            product_sales[product_id] = product_sales.get(product_id, 0) + order['quantity']

    filename = f'Daily_report_{datetime.now().strftime("%Y-%m-%d")}.txt'
    with open(filename, 'w') as file:
        file.write(f"Daily Report - {today}\n")
        file.write(f"Total Orders: {daily_orders}\n")
        file.write(f"Total Sales: ${daily_sales:.2f}\n")
        file.write("Products sold today:\n")
        for product_id, quantity_sold in product_sales.items():
            file.write(f"Product ID: {product_id}, Units Sold: {quantity_sold}\n")

    print(f"Daily report made: {filename}")
    
schedule.every().day.at("00:00").do(create_daily_report)
while True:
    schedule.run_pending()
    time.sleep(1)
