-- Task 1
SELECT orders.order_id, orders.customer_id, orders.order_date, orders.product_id, orders.quantity, products.product_name, products.category, products.price, customers.customer_name, customers.email FROM sales.orders as orders
JOIN sales.products as products
ON orders.product_id = products.product_id
JOIN sales.customers as customers
ON orders.customer_id = customers.customer_id

-- Task 2
SELECT product_id, product_name, total_sales
FROM (
    SELECT 
        p.product_id,
        p.product_name,
        SUM(o.quantity * p.price) AS total_sales
    FROM sales.orders AS o
    JOIN sales.products AS p ON o.product_id = p.product_id
    WHERE o.order_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'
    GROUP BY p.product_id, p.product_name
) AS subquery
ORDER BY total_sales DESC
LIMIT 5;

-- Task 3
SELECT 
    o.order_id,
    o.customer_id,
    o.order_date,
    o.product_id,
    o.quantity,
    p.product_name,
    p.category,
    p.price,
    c.customer_name,
    c.email,
    c.join_date,
    (o.quantity * p.price) AS total_order_value,
    CASE
        WHEN (o.quantity * p.price) >= 1000 THEN 'High'
        WHEN (o.quantity * p.price) BETWEEN 500 AND 999 THEN 'Medium'
        ELSE 'Low'
    END AS revenue_category
FROM sales.orders AS o
JOIN sales.products AS p ON o.product_id = p.product_id
JOIN sales.customers AS c ON o.customer_id = c.customer_id;

-- Task 4
EXPLAIN ANALYZE
SELECT 
    orders.order_id, 
    orders.customer_id, 
    orders.order_date, 
    orders.product_id, 
    orders.quantity, 
    products.product_name, 
    products.category, 
    products.price, 
    customers.customer_name, 
    customers.email, 
    customers.join_date 
FROM sales.orders AS orders
JOIN sales.products AS products ON orders.product_id = products.product_id
JOIN sales.customers AS customers ON orders.customer_id = customers.customer_id;