import csv
import sqlite3

conn = sqlite3.connect('winterproducts2.db')
cursor = conn.cursor()

cursor.execute('''
    CREATE TABLE IF NOT EXISTS products (
        product_id INTEGER PRIMARY KEY,
        product_name TEXT,
        quantity INTEGER,
        sale_price_USD INTEGER
    )
''')

with open('productlist.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        product_data = (
            int(row['product_id']),
            row['product_name'],
            int(row['quantity']),
            int(row['sale_price_USD'])
        )
        cursor.execute('''
            INSERT INTO products (product_id, product_name, quantity, sale_price_USD)
            VALUES (?, ?, ?, ?)
        ''', product_data)

conn.commit()
conn.close()

print("Database created.")
