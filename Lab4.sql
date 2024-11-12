-- Task 2
CREATE INDEX idx_orders_customer_id ON sales.orders (customer_id);
CREATE INDEX idx_orders_order_date ON sales.orders (order_date);
CREATE INDEX idx_orders_product_id ON sales.orders (product_id);
CREATE INDEX idx_orders_order_id ON sales.orders (order_id);
CREATE INDEX idx_products_product_id ON sales.products (product_id);

-- Task 3

ALTER TABLE sales.inventory
ADD COLUMN supplier_id INTEGER;

ALTER TABLE sales.inventory
ADD CONSTRAINT fk_supplier
FOREIGN KEY (supplier_id) REFERENCES sales.suppliers(supplier_id);

-- Create the supplier_info table for normalization
CREATE TABLE IF NOT EXISTS sales.supplier_info (
    info_id SERIAL PRIMARY KEY,
    supplier_id INTEGER,
    supplier_address VARCHAR(50),
    email VARCHAR(50),
    contact_number VARCHAR(50),
    fax VARCHAR(50),
    account_number VARCHAR(50),
    order_history VARCHAR(50),
    contract VARCHAR(50),
    FOREIGN KEY (supplier_id) REFERENCES sales.suppliers(supplier_id)
);

-- Drop columns from supplier after inserting the data into supplier_info
ALTER TABLE sales.suppliers 
DROP COLUMN supplier_address, 
DROP COLUMN email, 
DROP COLUMN contact_number, 
DROP COLUMN fax, 
DROP COLUMN account_number, 
DROP COLUMN order_history, 
DROP COLUMN contract

-- Task 4
CREATE TABLE sales.orders_partitioned (
    order_id SERIAL,
    customer_id INTEGER,
    order_date DATE,
    product_id INTEGER,
    quantity INTEGER,
    PRIMARY KEY (order_id, order_date)
)
PARTITION BY RANGE (order_date);

CREATE TABLE sales.orders_2024 PARTITION OF sales.orders_partitioned
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

CREATE TABLE sales.orders_2025 PARTITION OF sales.orders_partitioned
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

INSERT INTO sales.orders_partitioned (order_id, customer_id, order_date, product_id, quantity)
SELECT order_id, customer_id, order_date, product_id, quantity
FROM sales.orders;

DROP TABLE sales.orders;

ALTER TABLE sales.orders_partitioned RENAME TO orders;