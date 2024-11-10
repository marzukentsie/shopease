import pandas as pd
import numpy as np


orders_df = pd.read_csv('Orders.csv',index_col=0)
products_df = pd.read_csv('products.csv')
orders_items_df = pd.read_csv('order_items.csv',index_col=0)
customers_df = pd.read_csv('customers.csv')
inventory_df = pd.read_csv('inventory.csv')
suppliers_df = pd.read_csv('suppliers_data.csv')

dataframes = [orders_df, products_df, orders_items_df, customers_df, inventory_df, suppliers_df]

# # Clean all the dataframes
# # date columns available in the dataframes order_date | join_date | stock_date
date_columns = ['order_date', 'join_date', 'stock_date']

for df in dataframes:
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)
    # if date column exists, convert it to datetime
    if any(col in df.columns for col in date_columns):
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])

# # orders columns: order_id,Cutomer_id,order_date,product_id,quantity
# # products columns: product_id,product_name,category,price
# # order_items columns: order_detail_id,order_id,quantity,product_id
# # customers columns: customer_id,customer_name,email,join_date
# # inventory columns: product_name,stock_quantity,stock_date,supplier,warehouse_location
# # suppliers columns: supplier_name,supplier_address,email,contact_number,fax,account_number,order_history,contract,supplier_country,supplier_city,country_code

# # verify the dataframes
# # for df in dataframes:
# #     print(df.shape)
print(orders_df.head())

import psycopg2 as pg
from dotenv import load_dotenv
import os


load_dotenv('.env.local')
connectionString = os.getenv('POSTGRESQL_CRED')
connection = pg.connect(connectionString)
cursor = connection.cursor()

# # Create tables in sales schema

# # cursor.execute('''
# # CREATE TABLE IF NOT EXISTS sales.orders (
# #     order_id SERIAL PRIMARY KEY,
# #     customer_id INTEGER,
# #     order_date DATE,
# #     product_id INTEGER,
# #     quantity INTEGER
# # );
# # ''')

# cursor.execute('''
# CREATE TABLE IF NOT EXISTS sales.products (
#     product_id INTEGER PRIMARY KEY,
#     product_name VARCHAR(50),
#     category VARCHAR(50),
#     price DECIMAL
# );
# ''')

# # cursor.execute('''
# # CREATE TABLE IF NOT EXISTS sales.order_items (
# #     order_detail_id SERIAL PRIMARY KEY,
# #     order_id INTEGER,
# #     quantity INTEGER,
# #     product_id INTEGER
# # );
# # ''')

# cursor.execute('''
# CREATE TABLE IF NOT EXISTS sales.customers (
#     customer_id INTEGER PRIMARY KEY,
#     customer_name VARCHAR(50),
#     email VARCHAR(50),
#     join_date DATE
# );
# ''')

# # cursor.execute('''
# # CREATE TABLE IF NOT EXISTS sales.inventory (
# #     product_name VARCHAR(50),
# #     stock_quantity INTEGER,
# #     stock_date DATE,
# #     supplier VARCHAR(50),
# #     warehouse_location VARCHAR(50)
# # );
# # ''')

# # cursor.execute('''
# # CREATE TABLE IF NOT EXISTS sales.suppliers (
# #     supplier_id SERIAL PRIMARY KEY,
# #     supplier_name VARCHAR(50),
# #     supplier_address VARCHAR(50),
# #     email VARCHAR(50),
# #     contact_number VARCHAR(50),
# #     fax VARCHAR(50),
# #     account_number VARCHAR(50),
# #     order_history VARCHAR(50),
# #     contract VARCHAR(50),
# #     supplier_country VARCHAR(50),
# #     supplier_city VARCHAR(50),
# #     country_code VARCHAR(50)
# # );
# # ''')

# # Insert data into the tables

# # for index, row in orders_df.iterrows():
# #     cursor.execute('''
# #     INSERT INTO sales.orders (customer_id, order_date, product_id, quantity)
# #     VALUES (%s, %s, %s, %s)
# #     ''', (row['Cutomer_id'], row['order_date'], row['product_id'], row['quantity']))

# for index, row in products_df.iterrows():
#     cursor.execute('''
#     INSERT INTO sales.products (product_id, product_name, category, price)
#     VALUES (%s, %s, %s, %s)
#     ''', (row['product_id'],row['product_name'], row['category'], row['price']))

# # print(orders_items_df.dtypes)
# # for index, row in orders_items_df.iterrows():
# #     cursor.execute('''
# #     INSERT INTO sales.order_items (order_id, quantity, product_id)
# #     VALUES (%s, %s, %s)
# #     ''', (int(row['order_id']), int(row['quantity']), int(row['product_id'])))

# for index, row in customers_df.iterrows():
#     cursor.execute('''
#     INSERT INTO sales.customers (customer_id, customer_name, email, join_date)
#     VALUES (%s, %s, %s, %s)
#     ''', (row['customer_id'], row['customer_name'], row['email'], row['join_date']))

# # print(inventory_df.columns.str.strip())
# # print(inventory_df.head())
# # for index, row in inventory_df.iterrows():
# #     cursor.execute('''
# #     INSERT INTO sales.inventory (product_name, stock_quantity, stock_date, supplier, warehouse_location)
# #     VALUES (%s, %s, %s, %s, %s)
# #     ''', (row['product_name'], row['stock_quantity'], row['stock_date'], row['supplier'], row['warehouse_location']))

# # for index, row in suppliers_df.iterrows():
# #     cursor.execute('''
# #     INSERT INTO sales.suppliers (supplier_name, supplier_address, email, contact_number, fax, account_number, order_history, contract, supplier_country, supplier_city, country_code)
# #     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
# #     ''', (row['supplier_name'], row['supplier_address'], row['email'], row['contact_number'], row['fax'], row['account_number'], row['order_history'], row['contract'], row['supplier_country'], row['supplier_city'], row['country_code']))

connection.commit()

connection.close()

# JOIN Operations: Write SQL queries to join the orders, products, and customers tables 
# to get a comprehensive view of each order.
# query to join orders, products and customers tables
# cursor.execute('''
# SELECT orders.order_id, orders.customer_id, orders.order_date, orders.product_id, orders.quantity, products.product_name, products.category, products.price, customers.customer_name, customers.email, customers.join_date FROM sales.orders as orders
# JOIN sales.products as products
# ON orders.product_id = products.product_id
# JOIN sales.customers as customers
# ON orders.customer_id = customers.customer_id
# ''')

# data = cursor.fetchall()
# print(data)

# connection.close()