-- ============================================================================
-- REAL-TIME CDC PLATFORM - SOURCE SCHEMA
-- Fast-moving transactional data with CDC enabled
-- ============================================================================

-- Enable logical decoding for CDC
CREATE EXTENSION IF NOT EXISTS hstore;

-- CUSTOMERS TABLE
CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(20),
    address VARCHAR(500),
    city VARCHAR(100),
    country VARCHAR(100),
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- PRODUCTS TABLE
CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    price DECIMAL(10, 2) NOT NULL,
    stock_quantity INT DEFAULT 0,
    supplier_id INT,
    status VARCHAR(50) DEFAULT 'available',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ORDERS TABLE (High Volume - CDC Target)
CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES customers(customer_id),
    order_date TIMESTAMP NOT NULL,
    total_amount DECIMAL(12, 2) NOT NULL,
    status VARCHAR(50) DEFAULT 'pending',
    payment_method VARCHAR(50),
    shipping_address VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ORDER_ITEMS TABLE (Detailed Line Items)
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INT NOT NULL REFERENCES orders(order_id),
    product_id INT NOT NULL REFERENCES products(product_id),
    quantity INT NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    discount DECIMAL(5, 2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- RETURNS TABLE (Warranty & Return Management)
CREATE TABLE IF NOT EXISTS returns (
    return_id SERIAL PRIMARY KEY,
    order_id INT NOT NULL REFERENCES orders(order_id),
    product_id INT NOT NULL REFERENCES products(product_id),
    return_reason VARCHAR(500),
    status VARCHAR(50) DEFAULT 'pending',
    refund_amount DECIMAL(10, 2),
    return_date TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- INVENTORY MOVEMENTS TABLE (for real-time inventory tracking)
CREATE TABLE IF NOT EXISTS inventory_movements (
    movement_id SERIAL PRIMARY KEY,
    product_id INT NOT NULL REFERENCES products(product_id),
    movement_type VARCHAR(50),
    quantity_changed INT NOT NULL,
    reason VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- CREATE INDEXES FOR CDC PERFORMANCE
CREATE INDEX idx_orders_customer_date ON orders(customer_id, order_date DESC);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_order_items_order ON order_items(order_id);
CREATE INDEX idx_order_items_product ON order_items(product_id);
CREATE INDEX idx_returns_order ON returns(order_id);
CREATE INDEX idx_inventory_product ON inventory_movements(product_id);

-- REPLICATION SLOT FOR CDC
SELECT * FROM pg_create_logical_replication_slot('cdc_slot', 'test_decoding');

-- Create publication for CDC tables
CREATE PUBLICATION cdc_pub FOR TABLE customers, products, orders, order_items, returns, inventory_movements;

-- GRANT PERMISSIONS
GRANT CONNECT ON DATABASE transactional_db TO source_user;
GRANT USAGE ON SCHEMA public TO source_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO source_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO source_user;
