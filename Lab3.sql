-- Task 1
SELECT
    product_id,
    product_name,
    total_revenue,
    ROW_NUMBER() OVER (ORDER BY total_revenue DESC) AS row_num
FROM
    sales.sales;

SELECT
    product_id,
    product_name,
    total_revenue,
    RANK() OVER (ORDER BY total_revenue DESC) AS rank_col
FROM
    sales.sales;

SELECT
    product_id,
    product_name,
    total_revenue,
    DENSE_RANK() OVER (ORDER BY total_revenue DESC) AS dense_rank
FROM
    sales.sales;

-- Task 2
SELECT 
    category,
    order_date,
    total_revenue,
    SUM(total_revenue) OVER (PARTITION BY category ORDER BY order_date) AS running_total
FROM 
    sales.sales
ORDER BY 
    category, order_date;

-- Task 3
SELECT 
    year,
    month,
    SUM(total_revenue) AS monthly_sales,
    LAG(SUM(total_revenue)) OVER (ORDER BY year, month) AS previous_month_sales,
    LEAD(SUM(total_revenue)) OVER (ORDER BY year, month) AS next_month_sales
FROM 
    sales.sales
GROUP BY 
    year, month
ORDER BY 
    year, month;

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
        s.order_id,
        s.customer_id,
        c.customer_name,
        s.order_date,
        SUM(s.quantity * p.price) AS total_order_value
    FROM sales.sales AS s
    JOIN sales.products AS p ON s.product_id = p.product_id
    JOIN sales.customers AS c ON s.customer_id = c.customer_id
    GROUP BY s.order_id, s.customer_id, c.customer_name, s.order_date
) AS subquery
ORDER BY customer_id, order_date;