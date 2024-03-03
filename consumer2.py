from kafka import KafkaConsumer
import json
from datetime import datetime, timezone, timedelta

bootstrap_servers = ['localhost:9092']
topic_name = 'onlineshop'

consumer = KafkaConsumer(topic_name,
                         bootstrap_servers=bootstrap_servers,
                         auto_offset_reset='earliest',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                         consumer_timeout_ms=10000)

def track_sales():
    total_sales_today = 0.0
    sales_last_hour = 0.0
    sales_data = []

    start_of_day = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)

    for message in consumer:
        order = message.value
        order_timestamp = datetime.fromtimestamp(order['timestamp'], tz=timezone.utc)

        if 'sale_price_USD' in order:
            saleprice = order['sale_price_USD']
        else:
            continue

        if order_timestamp >= start_of_day:
            total_sales_today += order['quantity'] * saleprice
            sales_data.append((order_timestamp, order['quantity'] * saleprice))

        sales_last_hour = sum(sale for time, sale in sales_data if time > one_hour_ago)

        print(f"Total sales today: ${total_sales_today:.2f}")
        print(f"Sales in the last hour: ${sales_last_hour:.2f}")

if __name__ == '__main__':
    track_sales()
