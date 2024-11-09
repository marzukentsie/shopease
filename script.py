import pandas as pd
import numpy as np

# Load the data
orders_df = pd.read_csv('Orders.csv')
products_df = pd.read_csv('products.csv')
# orders_items_df = pd.read_csv('order_items.csv')
# customers_df = pd.read_csv('customers.csv')
# inventory_df = pd.read_csv('inventory.csv')
# suppliers_df = pd.read_csv('suppliers_data.csv')

dataFrames = {
    "orders": orders_df,
    # "order_items": orders_items_df,
    # "customers": customers_df,
    # "inventory": inventory_df,
    # "suppliers": suppliers_df,
    "products": products_df
}
sales_df = orders_df.merge(products_df, on='product_id')
# dataFrames.update({"sales": sales_df})
# Data Cleaning: Handle missing values, remove duplicates, and convert data types appropriately
# Make your own sales dataframe that will have the following columns: order_id, order_date, customer_id, product_id, quantity, and price
sales_df.dropna(inplace=True)
sales_df.drop_duplicates(inplace=True)
sales_df['order_date'] = pd.to_datetime(sales_df['order_date'])
# NumPy Operations: Use NumPy to calculate the total revenue for each order and add it
# as a new column in the DataFrame.
sales_df['total_revenue']  = np.multiply(sales_df['quantity'], sales_df['price'])
# print(sales_df.head())
# 5. Visualization: Plot the monthly sales trend using Pandas’ plotting capabilities.

# Step 4: Data Transformation - Extract year, month, and day from 'order_date'
sales_df['year'] = sales_df['order_date'].dt.year   
sales_df['month'] = sales_df['order_date'].dt.month  

# for year in sales_df['year'].unique():
#     print(f"Year: {year}")

# for month in sales_df['month'].unique():
#     print(f"Month: {month}")

# 5. Visualization: Plot the monthly sales trend using Pandas’ plotting capabilities. do not use matplotlib
monthly_sales = sales_df.groupby(['year', 'month'])['total_revenue'].sum()
# monthly_sales.plot(kind='bar', title='Monthly Sales Trend')
# print(monthly_sales)
# calculate the total revenue for each month and print them use the sales_df

import psycopg2 as pg
from dotenv import load_dotenv
import os


load_dotenv('.env.local')
# Loading Data: Populate the data into your database. Do not forget to create the schema
# in your database before populating the data with python.
# # Step 1: Connect to the database
connectionString = os.getenv('POSTGRESQL_CRED')
connection = pg.connect(connectionString)
cursor = connection.cursor()

# print without index
# print(sales_df.to_string(index=False))
# Step 2: Create the schema
# print(sales_df.dtypes)
# cursor.execute(''' 
# CREATE SCHEMA IF NOT EXISTS sales;
# ''')
# Step 3: Create the table
# Sales Table
# cursor.execute('''
# CREATE TABLE IF NOT EXISTS sales.sales (
#     order_id INT,
#     customer_id INT,
#     order_date DATE,
#     product_id INT,
#     quantity INT,
#     product_name VARCHAR(50),
#     category VARCHAR(50),
#     price FLOAT,
#     total_revenue FLOAT,
#     year INT,
#     month INT
# );
# ''')

# Step 4: Insert the data
# for index, row in sales_df.iterrows():
#     cursor.execute('''
#     INSERT INTO sales.sales (order_id, customer_id, order_date, product_id, quantity, product_name, category, price, total_revenue, year, month)
#     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
#     ''', (row['order_id'], row['Cutomer_id'], row['order_date'], row['product_id'], row['quantity'], row['product_name'], row['category'], row['price'], row['total_revenue'], row['year'], row['month']))

# connection.commit()
# # view what the itterrows() method does
# for index, row in sales_df.iterrows():
#     # print(index)
#     print(row)
#     break
# print(sales_df.columns)

# Select the data from the table
cursor.execute('''
SELECT * FROM sales.sales;
''')

def view_table(table_name, limit):
    query = f"SELECT * FROM {table_name}"
    if limit:
        query += f" LIMIT {limit}"
    return pd.read_sql_query(query, connection)

print(view_table('sales.sales', 10))
# Fetch the data
connection.close()