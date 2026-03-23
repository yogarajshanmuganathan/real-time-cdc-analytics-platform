-- ============================================================================
-- SCD TYPE 2 PROCEDURES - HANDLING DIMENSION CHANGES FROM CDC
-- ============================================================================

-- PROCEDURE: Load/Upsert Customers Dimension with SCD Type 2
CREATE OR REPLACE PROCEDURE sp_load_dim_customers(
    p_customer_id INT,
    p_name VARCHAR,
    p_email VARCHAR,
    p_phone VARCHAR,
    p_city VARCHAR,
    p_country VARCHAR,
    p_status VARCHAR,
    p_source_timestamp TIMESTAMP
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_current_sk INT;
    v_row_exists BOOLEAN;
    v_changed BOOLEAN := FALSE;
BEGIN
    -- Check if current record exists
    SELECT customer_sk INTO v_current_sk
    FROM dim_customers
    WHERE customer_id = p_customer_id AND is_current = TRUE;

    IF v_current_sk IS NULL THEN
        -- INSERT new dimension record
        INSERT INTO dim_customers (
            customer_id, name, email, phone, city, country, status,
            effective_date, is_current, source_timestamp
        ) VALUES (
            p_customer_id, p_name, p_email, p_phone, p_city, p_country, p_status,
            CURRENT_TIMESTAMP, TRUE, p_source_timestamp
        );
        RAISE NOTICE 'Inserted new customer record: %', p_customer_id;
    ELSE
        -- Check if any attributes changed
        SELECT EXISTS (
            SELECT 1 FROM dim_customers
            WHERE customer_sk = v_current_sk
            AND (
                name IS DISTINCT FROM p_name
                OR email IS DISTINCT FROM p_email
                OR phone IS DISTINCT FROM p_phone
                OR city IS DISTINCT FROM p_city
                OR country IS DISTINCT FROM p_country
                OR status IS DISTINCT FROM p_status
            )
        ) INTO v_changed;

        IF v_changed THEN
            -- Close current record
            UPDATE dim_customers
            SET end_date = CURRENT_TIMESTAMP, is_current = FALSE
            WHERE customer_sk = v_current_sk;

            -- Insert new version (SCD Type 2)
            INSERT INTO dim_customers (
                customer_id, name, email, phone, city, country, status,
                effective_date, is_current, source_timestamp
            ) VALUES (
                p_customer_id, p_name, p_email, p_phone, p_city, p_country, p_status,
                CURRENT_TIMESTAMP, TRUE, p_source_timestamp
            );
            RAISE NOTICE 'Updated customer record (SCD Type 2): %', p_customer_id;
        ELSE
            RAISE NOTICE 'No changes detected for customer %', p_customer_id;
        END IF;
    END IF;
END;
$$;

-- PROCEDURE: Load/Upsert Products Dimension with SCD Type 2
CREATE OR REPLACE PROCEDURE sp_load_dim_products(
    p_product_id INT,
    p_name VARCHAR,
    p_category VARCHAR,
    p_price DECIMAL,
    p_status VARCHAR,
    p_source_timestamp TIMESTAMP
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_current_sk INT;
    v_changed BOOLEAN := FALSE;
BEGIN
    SELECT product_sk INTO v_current_sk
    FROM dim_products
    WHERE product_id = p_product_id AND is_current = TRUE;

    IF v_current_sk IS NULL THEN
        INSERT INTO dim_products (
            product_id, name, category, price, status,
            effective_date, is_current, source_timestamp
        ) VALUES (
            p_product_id, p_name, p_category, p_price, p_status,
            CURRENT_TIMESTAMP, TRUE, p_source_timestamp
        );
        RAISE NOTICE 'Inserted new product record: %', p_product_id;
    ELSE
        SELECT EXISTS (
            SELECT 1 FROM dim_products
            WHERE product_sk = v_current_sk
            AND (
                name IS DISTINCT FROM p_name
                OR category IS DISTINCT FROM p_category
                OR price IS DISTINCT FROM p_price
                OR status IS DISTINCT FROM p_status
            )
        ) INTO v_changed;

        IF v_changed THEN
            UPDATE dim_products
            SET end_date = CURRENT_TIMESTAMP, is_current = FALSE
            WHERE product_sk = v_current_sk;

            INSERT INTO dim_products (
                product_id, name, category, price, status,
                effective_date, is_current, source_timestamp
            ) VALUES (
                p_product_id, p_name, p_category, p_price, p_status,
                CURRENT_TIMESTAMP, TRUE, p_source_timestamp
            );
            RAISE NOTICE 'Updated product record (SCD Type 2): %', p_product_id;
        END IF;
    END IF;
END;
$$;

-- PROCEDURE: Load Order Facts
CREATE OR REPLACE PROCEDURE sp_load_facts_orders(
    p_order_id INT,
    p_customer_id INT,
    p_order_date TIMESTAMP,
    p_status VARCHAR,
    p_total_items INT,
    p_subtotal DECIMAL,
    p_discount DECIMAL,
    p_final_amount DECIMAL,
    p_source_timestamp TIMESTAMP
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_customer_sk INT;
    v_date_sk INT;
    v_status_sk INT;
BEGIN
    -- Lookup dimension keys
    SELECT customer_sk INTO v_customer_sk
    FROM dim_customers
    WHERE customer_id = p_customer_id AND is_current = TRUE
    LIMIT 1;

    SELECT date_sk INTO v_date_sk
    FROM dim_date
    WHERE date = DATE(p_order_date);

    SELECT status_sk INTO v_status_sk
    FROM dim_order_status
    WHERE status_code = p_status;

    IF v_customer_sk IS NULL OR v_date_sk IS NULL OR v_status_sk IS NULL THEN
        RAISE WARNING 'Dimension lookup failed - Customer: %, Date: %, Status: %', 
            v_customer_sk, v_date_sk, v_status_sk;
        RETURN;
    END IF;

    -- Upsert order fact
    INSERT INTO facts_orders (
        order_id, customer_sk, date_sk, status_sk,
        total_items, subtotal, discount_amount, final_amount,
        source_timestamp
    ) VALUES (
        p_order_id, v_customer_sk, v_date_sk, v_status_sk,
        p_total_items, p_subtotal, p_discount, p_final_amount,
        p_source_timestamp
    )
    ON CONFLICT (order_id) DO UPDATE SET
        status_sk = EXCLUDED.status_sk,
        final_amount = EXCLUDED.final_amount,
        load_timestamp = CURRENT_TIMESTAMP;

    RAISE NOTICE 'Loaded order fact: %', p_order_id;
END;
$$;

-- PROCEDURE: Load Order Item Facts
CREATE OR REPLACE PROCEDURE sp_load_facts_order_items(
    p_order_id INT,
    p_order_item_id INT,
    p_product_id INT,
    p_order_date TIMESTAMP,
    p_quantity INT,
    p_unit_price DECIMAL,
    p_discount DECIMAL,
    p_source_timestamp TIMESTAMP
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_product_sk INT;
    v_date_sk INT;
    v_line_amount DECIMAL;
    v_net_amount DECIMAL;
BEGIN
    SELECT product_sk INTO v_product_sk
    FROM dim_products
    WHERE product_id = p_product_id AND is_current = TRUE
    LIMIT 1;

    SELECT date_sk INTO v_date_sk
    FROM dim_date
    WHERE date = DATE(p_order_date);

    IF v_product_sk IS NULL OR v_date_sk IS NULL THEN
        RAISE WARNING 'Dimension lookup failed for order item - Product: %, Date: %',
            v_product_sk, v_date_sk;
        RETURN;
    END IF;

    v_line_amount := p_quantity * p_unit_price;
    v_net_amount := v_line_amount - (p_discount * p_quantity);

    INSERT INTO facts_order_items (
        order_id, order_item_id, product_sk, date_sk,
        quantity, unit_price, line_amount, discount_per_unit,
        discount_amount, net_amount, source_timestamp
    ) VALUES (
        p_order_id, p_order_item_id, v_product_sk, v_date_sk,
        p_quantity, p_unit_price, v_line_amount, p_discount,
        p_discount * p_quantity, v_net_amount, p_source_timestamp
    )
    ON CONFLICT (order_item_id) DO UPDATE SET
        quantity = EXCLUDED.quantity,
        unit_price = EXCLUDED.unit_price,
        line_amount = EXCLUDED.line_amount,
        discount_amount = EXCLUDED.discount_amount,
        net_amount = EXCLUDED.net_amount,
        load_timestamp = CURRENT_TIMESTAMP;

    RAISE NOTICE 'Loaded order item fact: %', p_order_item_id;
END;
$$;

-- PROCEDURE: Load Return Facts
CREATE OR REPLACE PROCEDURE sp_load_facts_returns(
    p_return_id INT,
    p_order_id INT,
    p_product_id INT,
    p_customer_id INT,
    p_return_date TIMESTAMP,
    p_refund_amount DECIMAL,
    p_return_reason VARCHAR,
    p_source_timestamp TIMESTAMP
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_product_sk INT;
    v_customer_sk INT;
    v_date_sk INT;
    v_days_to_return INT;
BEGIN
    SELECT product_sk INTO v_product_sk
    FROM dim_products
    WHERE product_id = p_product_id AND is_current = TRUE
    LIMIT 1;

    SELECT customer_sk INTO v_customer_sk
    FROM dim_customers
    WHERE customer_id = p_customer_id AND is_current = TRUE
    LIMIT 1;

    SELECT date_sk INTO v_date_sk
    FROM dim_date
    WHERE date = DATE(p_return_date);

    IF v_product_sk IS NULL OR v_customer_sk IS NULL OR v_date_sk IS NULL THEN
        RAISE WARNING 'Dimension lookup failed for return - Product: %, Customer: %, Date: %',
            v_product_sk, v_customer_sk, v_date_sk;
        RETURN;
    END IF;

    -- Calculate days to return (from order to return)
    SELECT EXTRACT(DAY FROM DATE(p_return_date) - DATE(o.order_date))::INT
    INTO v_days_to_return
    FROM orders o
    WHERE o.order_id = p_order_id;

    INSERT INTO facts_returns (
        return_id, order_id, product_sk, customer_sk, date_sk,
        refund_amount, return_reason, daysn_to_return, source_timestamp
    ) VALUES (
        p_return_id, p_order_id, v_product_sk, v_customer_sk, v_date_sk,
        p_refund_amount, p_return_reason, v_days_to_return, p_source_timestamp
    )
    ON CONFLICT (return_id) DO UPDATE SET
        return_reason = EXCLUDED.return_reason,
        load_timestamp = CURRENT_TIMESTAMP;

    RAISE NOTICE 'Loaded return fact: %', p_return_id;
END;
$$;

-- PROCEDURE: Vacuum and Refresh Materialized Views
CREATE OR REPLACE PROCEDURE sp_refresh_analytics_views()
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE 'Refreshing analytics views...';
    VACUUM ANALYZE facts_orders;
    VACUUM ANALYZE facts_order_items;
    VACUUM ANALYZE facts_returns;
    RAISE NOTICE 'Analytics views refreshed';
END;
$$;
