-- ============================================================================
-- PROCEDURES & FUNCTIONS FOR DATA GENERATION & BUSINESS LOGIC
-- ============================================================================

-- ===== DATA GENERATION PROCEDURES =====

-- Procedure to insert sample customers
CREATE OR REPLACE PROCEDURE generate_sample_customers(p_count INT DEFAULT 50)
LANGUAGE plpgsql
AS $$
DECLARE
    i INT;
    v_first_name VARCHAR;
    v_last_name VARCHAR;
    v_cities VARCHAR[] := ARRAY['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Miami', 'Seattle'];
    v_countries VARCHAR[] := ARRAY['USA', 'Canada', 'UK', 'Germany', 'France'];
BEGIN
    FOR i IN 1..p_count LOOP
        INSERT INTO customers (name, email, phone, address, city, country, status) VALUES (
            (ARRAY['John', 'Jane', 'Robert', 'Mary', 'Michael', 'Sarah', 'David', 'Emma'])[floor(random()*8+1)::int] || ' ' ||
            (ARRAY['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis'])[floor(random()*8+1)::int],
            'customer_' || i || '@example.com',
            '+1-' || floor(200 + random() * 800)::text || '-' || floor(100 + random() * 900)::text || '-' || lpad(floor(random() * 10000)::text, 4, '0'),
            floor(random() * 9999)::text || ' Main Street',
            v_cities[floor(random()*array_length(v_cities, 1)+1)::int],
            v_countries[floor(random()*array_length(v_countries, 1)+1)::int],
            (ARRAY['active', 'inactive', 'pending'])[floor(random()*3+1)::int]
        ) ON CONFLICT (email) DO NOTHING;
    END LOOP;
    RAISE NOTICE 'Generated % sample customers', p_count;
END;
$$;

-- Procedure to insert sample products
CREATE OR REPLACE PROCEDURE generate_sample_products(p_count INT DEFAULT 100)
LANGUAGE plpgsql
AS $$
DECLARE
    i INT;
    v_categories VARCHAR[] := ARRAY['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports', 'Toys', 'Food'];
BEGIN
    FOR i IN 1..p_count LOOP
        INSERT INTO products (name, category, price, stock_quantity, status) VALUES (
            'Product_' || i || '_' || substr(md5(random()::text), 1, 5),
            v_categories[floor(random()*array_length(v_categories, 1)+1)::int],
            (10 + random() * 490)::DECIMAL(10, 2),
            floor(random() * 1000)::int,
            (ARRAY['available', 'discontinued'])[floor(random()*2+1)::int]
        );
    END LOOP;
    RAISE NOTICE 'Generated % sample products', p_count;
END;
$$;

-- Procedure to insert sample orders
CREATE OR REPLACE PROCEDURE generate_sample_orders(p_count INT DEFAULT 200)
LANGUAGE plpgsql
AS $$
DECLARE
    i INT;
    v_customer_id INT;
    v_total_amount DECIMAL(12, 2);
    v_order_id INT;
    j INT;
    v_product_id INT;
BEGIN
    FOR i IN 1..p_count LOOP
        -- Select random customer
        SELECT customer_id INTO v_customer_id FROM customers ORDER BY RANDOM() LIMIT 1;
        
        -- Create order
        INSERT INTO orders (customer_id, order_date, total_amount, status, payment_method)
        VALUES (
            v_customer_id,
            CURRENT_TIMESTAMP - INTERVAL '1 day' * floor(random() * 30)::int,
            0,  -- Will be updated after adding items
            (ARRAY['pending', 'confirmed', 'shipped', 'delivered', 'cancelled'])[floor(random()*5+1)::int],
            (ARRAY['credit_card', 'paypal', 'bank_transfer', 'debit_card'])[floor(random()*4+1)::int]
        ) RETURNING order_id INTO v_order_id;
        
        -- Add 1-5 items per order
        v_total_amount := 0;
        FOR j IN 1..floor(random() * 4 + 1)::int LOOP
            SELECT product_id INTO v_product_id FROM products ORDER BY RANDOM() LIMIT 1;
            INSERT INTO order_items (order_id, product_id, quantity, unit_price, discount)
            VALUES (
                v_order_id,
                v_product_id,
                floor(random() * 5 + 1)::int,
                (10 + random() * 490)::DECIMAL(10, 2),
                (random() * 20)::DECIMAL(5, 2)
            );
        END LOOP;
        
        -- Update order total
        UPDATE orders SET total_amount = (SELECT SUM((unit_price * quantity) - discount) FROM order_items WHERE order_id = v_order_id)
        WHERE order_id = v_order_id;
    END LOOP;
    RAISE NOTICE 'Generated % sample orders', p_count;
END;
$$;

-- ===== BUSINESS LOGIC PROCEDURES =====

-- Procedure to process a return (updates inventory, creates return record)
CREATE OR REPLACE PROCEDURE process_return(
    p_order_id INT,
    p_product_id INT,
    p_reason VARCHAR DEFAULT 'Customer requested'
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_order_item_id INT;
    v_unit_price DECIMAL(10, 2);
    v_quantity INT;
    v_refund_amount DECIMAL(10, 2);
BEGIN
    -- Get order item details
    SELECT order_item_id, unit_price, quantity 
    INTO v_order_item_id, v_unit_price, v_quantity
    FROM order_items 
    WHERE order_id = p_order_id AND product_id = p_product_id
    LIMIT 1;

    IF v_order_item_id IS NULL THEN
        RAISE EXCEPTION 'Order item not found';
    END IF;

    -- Calculate refund
    v_refund_amount := v_unit_price * v_quantity;

    -- Create return record
    INSERT INTO returns (order_id, product_id, return_reason, status, refund_amount, return_date)
    VALUES (p_order_id, p_product_id, p_reason, 'pending', v_refund_amount, CURRENT_TIMESTAMP);

    -- Update inventory
    INSERT INTO inventory_movements (product_id, movement_type, quantity_changed, reason)
    VALUES (p_product_id, 'return', v_quantity, 'Customer return from order ' || p_order_id);

    -- Update product stock
    UPDATE products SET stock_quantity = stock_quantity + v_quantity WHERE product_id = p_product_id;

    RAISE NOTICE 'Return processed: Order %, Product %, Refund Amount: %', p_order_id, p_product_id, v_refund_amount;
END;
$$;

-- Function to calculate order value (with discounts)
CREATE OR REPLACE FUNCTION calculate_order_value(p_order_id INT)
RETURNS TABLE (
    total_items INT,
    subtotal DECIMAL(12, 2),
    total_discount DECIMAL(12, 2),
    final_total DECIMAL(12, 2)
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        COUNT(*)::INT as total_items,
        SUM(unit_price * quantity)::DECIMAL(12, 2) as subtotal,
        SUM(discount * quantity)::DECIMAL(12, 2) as total_discount,
        (SUM(unit_price * quantity) - SUM(discount * quantity))::DECIMAL(12, 2) as final_total
    FROM order_items
    WHERE order_id = p_order_id;
END;
$$ LANGUAGE SQL;

-- Function to get customer lifetime value and order statistics
CREATE OR REPLACE FUNCTION get_customer_analytics(p_customer_id INT)
RETURNS TABLE (
    customer_name VARCHAR,
    total_orders INT,
    total_spent DECIMAL(12, 2),
    average_order_value DECIMAL(12, 2),
    last_order_date TIMESTAMP,
    total_items_purchased INT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        c.name,
        COUNT(DISTINCT o.order_id)::INT,
        COALESCE(SUM(o.total_amount), 0)::DECIMAL(12, 2),
        COALESCE(AVG(o.total_amount), 0)::DECIMAL(12, 2),
        MAX(o.order_date),
        COALESCE(SUM(oi.quantity), 0)::INT
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    LEFT JOIN order_items oi ON o.order_id = oi.order_id
    WHERE c.customer_id = p_customer_id
    GROUP BY c.customer_id, c.name;
END;
$$ LANGUAGE SQL;

-- Materialized View for Daily Order Summary (for real-time analytics)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_order_summary AS
SELECT
    DATE(o.order_date) as order_date,
    o.status,
    COUNT(DISTINCT o.order_id) as total_orders,
    COUNT(DISTINCT o.customer_id) as unique_customers,
    SUM(o.total_amount)::DECIMAL(12, 2) as total_revenue,
    AVG(o.total_amount)::DECIMAL(12, 2) as avg_order_value,
    COUNT(oi.order_item_id) as total_items
FROM orders o
LEFT JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY DATE(o.order_date), o.status;

CREATE INDEX idx_mv_daily_summary ON mv_daily_order_summary(order_date DESC, status);

-- TRIGGERS FOR AUTOMATIC UPDATED_AT TIMESTAMP
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_customers_updated_at BEFORE UPDATE ON customers FOR EACH ROW EXECUTE FUNCTION update_timestamp();
CREATE TRIGGER tr_products_updated_at BEFORE UPDATE ON products FOR EACH ROW EXECUTE FUNCTION update_timestamp();
CREATE TRIGGER tr_orders_updated_at BEFORE UPDATE ON orders FOR EACH ROW EXECUTE FUNCTION update_timestamp();
CREATE TRIGGER tr_order_items_updated_at BEFORE UPDATE ON order_items FOR EACH ROW EXECUTE FUNCTION update_timestamp();
CREATE TRIGGER tr_returns_updated_at BEFORE UPDATE ON returns FOR EACH ROW EXECUTE FUNCTION update_timestamp();
