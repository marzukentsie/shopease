-- Task 2
CREATE INDEX idx_orders_customer_id ON sales.orders (customer_id);
CREATE INDEX idx_orders_order_date ON sales.orders (order_date);
CREATE INDEX idx_orders_product_id ON sales.orders (product_id);
CREATE INDEX idx_orders_order_id ON sales.orders (order_id);
CREATE INDEX idx_products_product_id ON sales.products (product_id);
CREATE INDEX idx_customers_customer_id ON sales.customers (customer_id);

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
