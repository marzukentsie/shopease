-- Task 1
SELECT 
    product_id,
    product_name,
    total_sales,
    ROW_NUMBER() OVER (ORDER BY total_sales DESC) AS row_number
FROM (
    SELECT 
        p.product_id,
        p.product_name,
        SUM(o.quantity * p.price) AS total_sales
    FROM sales.orders AS o
    JOIN sales.products AS p ON o.product_id = p.product_id
    GROUP BY p.product_id, p.product_name
) AS subquery;

-- Task 2
SELECT 
    category,
    order_date,
    product_id,
    product_name,
    quantity,
    price,
    (quantity * price) AS total_sales,
    SUM(quantity * price) OVER (PARTITION BY category ORDER BY order_date) AS running_total
FROM (
    SELECT 
        o.order_date,
        p.product_id,
        p.product_name,
        p.category,
        o.quantity,
        p.price
    FROM sales.orders AS o
    JOIN sales.products AS p ON o.product_id = p.product_id
) AS subquery
ORDER BY category, order_date;

-- Task 3
SELECT 
    customer_id,
    customer_name,
    order_id,
    order_date,
    total_order_value,
    AVG(total_order_value) OVER (PARTITION BY customer_id) AS avg_order_value
FROM (
    SELECT 
        o.order_id,
        o.customer_id,
        c.customer_name,
        o.order_date,
        SUM(o.quantity * p.price) AS total_order_value
    FROM sales.orders AS o
    JOIN sales.products AS p ON o.product_id = p.product_id
    JOIN sales.customers AS c ON o.customer_id = c.customer_id
    GROUP BY o.order_id, o.customer_id, c.customer_name, o.order_date
) AS subquery
ORDER BY customer_id, order_date;

-- Task 4
SELECT 
    year,
    month,
    total_sales,
    LAG(total_sales) OVER (ORDER BY year, month) AS previous_month_sales,
    LEAD(total_sales) OVER (ORDER BY year, month) AS next_month_sales
FROM (
    SELECT 
        EXTRACT(YEAR FROM order_date) AS year,
        EXTRACT(MONTH FROM order_date) AS month,
        SUM(quantity * price) AS total_sales
    FROM sales.orders AS o
    JOIN sales.products AS p ON o.product_id = p.product_id
    GROUP BY EXTRACT(YEAR FROM order_date), EXTRACT(MONTH FROM order_date)
) AS subquery
ORDER BY year, month;

-- Task 5
SELECT 
    customer_id,
    customer_name,
    order_date,
    total_order_value,
    SUM(total_order_value) OVER (PARTITION BY customer_id ORDER BY order_date) AS running_total_sales,
    AVG(total_order_value) OVER (PARTITION BY customer_id) AS avg_order_value,
    LAG(total_order_value) OVER (PARTITION BY customer_id ORDER BY order_date) AS previous_order_value,
    LEAD(total_order_value) OVER (PARTITION BY customer_id ORDER BY order_date) AS next_order_value,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS order_number
FROM (
    SELECT 
        o.order_id,
        o.customer_id,
        c.customer_name,
        o.order_date,
        SUM(o.quantity * p.price) AS total_order_value
    FROM sales.orders AS o
    JOIN sales.products AS p ON o.product_id = p.product_id
    JOIN sales.customers AS c ON o.customer_id = c.customer_id
    GROUP BY o.order_id, o.customer_id, c.customer_name, o.order_date
) AS subquery
ORDER BY customer_id, order_date;