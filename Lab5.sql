--Task 1
CREATE OR REPLACE FUNCTION update_inventory_function()
RETURNS TRIGGER AS $$
BEGIN
    -- Check if there is sufficient stock
    IF (SELECT stock_quantity FROM sales.inventory WHERE product_id = NEW.product_id) < NEW.quantity THEN
        RAISE NOTICE 'Insufficient stock for product_id %', NEW.product_id;
        RETURN NULL;
    ELSE
        -- Decrease the product inventory count
        UPDATE sales.inventory
        SET stock_quantity = stock_quantity - NEW.quantity
        WHERE product_id = NEW.product_id;
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_inventory
AFTER INSERT ON sales.order_items
FOR EACH ROW
EXECUTE FUNCTION update_inventory_function();

--Task 2
CREATE OR REPLACE PROCEDURE update_customer_status(customer_id_param INTEGER)
LANGUAGE plpgsql
AS $$
DECLARE
    total_order_value NUMERIC;
BEGIN
    -- Calculate the total order value for the customer
    SELECT SUM(o.quantity * p.price) INTO total_order_value
    FROM sales.orders o
    JOIN sales.order_items oi ON o.order_id = oi.order_id
    JOIN sales.products p ON oi.product_id = p.product_id
    WHERE o.customer_id = customer_id_param;

    -- Update the customer status based on the total order value
    IF total_order_value > 10000 THEN
        UPDATE sales.customers
        SET status = 'VIP'
        WHERE customer_id = customer_id_param;
    ELSE
        UPDATE sales.customers
        SET status = 'Regular'
        WHERE customer_id = customer_id_param;
    END IF;
END;
$$;