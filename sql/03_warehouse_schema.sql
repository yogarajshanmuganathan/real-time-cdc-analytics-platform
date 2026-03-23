-- ============================================================================
-- WAREHOUSE SCHEMA - ANALYTICS LAYER (TARGET)
-- Kimball-style star schema with SCD Type 2 for dimension changes
-- ============================================================================

-- DIMENSION: DIM_CUSTOMERS (with SCD Type 2)
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_sk SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(20),
    city VARCHAR(100),
    country VARCHAR(100),
    status VARCHAR(50),
    
    -- SCD Type 2 Tracking
    effective_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_date TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE,
    
    -- Metadata
    source_timestamp TIMESTAMP,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT pk_dim_customers PRIMARY KEY (customer_sk)
);

CREATE INDEX idx_dim_customers_id ON dim_customers(customer_id);
CREATE INDEX idx_dim_customers_current ON dim_customers(is_current) WHERE is_current = TRUE;

-- DIMENSION: DIM_PRODUCTS (with SCD Type 2)
CREATE TABLE IF NOT EXISTS dim_products (
    product_sk SERIAL PRIMARY KEY,
    product_id INT NOT NULL,
    name VARCHAR(255),
    category VARCHAR(100),
    price DECIMAL(10, 2),
    status VARCHAR(50),
    
    -- SCD Type 2 Tracking
    effective_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_date TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE,
    
    -- Metadata
    source_timestamp TIMESTAMP,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT pk_dim_products PRIMARY KEY (product_sk)
);

CREATE INDEX idx_dim_products_id ON dim_products(product_id);
CREATE INDEX idx_dim_products_current ON dim_products(is_current) WHERE is_current = TRUE;

-- DIMENSION: DIM_DATE (Conformed Dimension)
CREATE TABLE IF NOT EXISTS dim_date (
    date_sk INT PRIMARY KEY,
    date DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    day_of_week INT NOT NULL,
    week_of_year INT NOT NULL,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN DEFAULT FALSE,
    date_string VARCHAR(10)
);

CREATE INDEX idx_dim_date_date ON dim_date(date);

-- Populate Dim_Date for 5 years (2024-2029)
INSERT INTO dim_date (date_sk, date, year, quarter, month, day, day_of_week, week_of_year, is_weekend, date_string)
SELECT 
    CAST(TO_CHAR(d, 'YYYYMMDD') AS INT) as date_sk,
    d as date,
    EXTRACT(YEAR FROM d)::INT as year,
    EXTRACT(QUARTER FROM d)::INT as quarter,
    EXTRACT(MONTH FROM d)::INT as month,
    EXTRACT(DAY FROM d)::INT as day,
    EXTRACT(DOW FROM d)::INT as day_of_week,
    EXTRACT(WEEK FROM d)::INT as week_of_year,
    (EXTRACT(DOW FROM d) IN (0, 6)) as is_weekend,
    TO_CHAR(d, 'YYYY-MM-DD') as date_string
FROM (
    SELECT DATE '2024-01-01' + (n || ' days')::INTERVAL AS d
    FROM generate_series(0, 1825) n
) dates
ON CONFLICT DO NOTHING;

-- DIMENSION: DIM_ORDER_STATUS
CREATE TABLE IF NOT EXISTS dim_order_status (
    status_sk SERIAL PRIMARY KEY,
    status_code VARCHAR(50),
    status_description VARCHAR(255),
    status_category VARCHAR(50)
);

INSERT INTO dim_order_status (status_code, status_description, status_category) VALUES
    ('pending', 'Order received, awaiting processing', 'New'),
    ('confirmed', 'Order confirmed and ready to ship', 'Processing'),
    ('shipped', 'Order shipped to customer', 'In Transit'),
    ('delivered', 'Order successfully delivered', 'Complete'),
    ('cancelled', 'Order cancelled by customer or system', 'Cancelled'),
    ('returned', 'Order returned by customer', 'Cancelled'),
    ('failed', 'Order process failed', 'Failed')
ON CONFLICT DO NOTHING;

-- FACT TABLE: FACTS_ORDERS
CREATE TABLE IF NOT EXISTS facts_orders (
    order_fact_sk SERIAL PRIMARY KEY,
    order_id INT NOT NULL,
    customer_sk INT NOT NULL REFERENCES dim_customers(customer_sk),
    date_sk INT NOT NULL REFERENCES dim_date(date_sk),
    status_sk INT NOT NULL REFERENCES dim_order_status(status_sk),
    
    -- Metrics
    total_items INT,
    subtotal DECIMAL(12, 2),
    discount_amount DECIMAL(12, 2),
    tax_amount DECIMAL(12, 2) DEFAULT 0,
    final_amount DECIMAL(12, 2),
    
    -- Aggregates
    item_count INT DEFAULT 1,
    
    -- Metadata
    source_timestamp TIMESTAMP,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT pk_facts_orders PRIMARY KEY (order_fact_sk),
    CONSTRAINT uq_order_id UNIQUE(order_id)
);

CREATE INDEX idx_facts_orders_customer ON facts_orders(customer_sk);
CREATE INDEX idx_facts_orders_date ON facts_orders(date_sk);
CREATE INDEX idx_facts_orders_status ON facts_orders(status_sk);

-- FACT TABLE: FACTS_ORDER_ITEMS
CREATE TABLE IF NOT EXISTS facts_order_items (
    order_item_fact_sk SERIAL PRIMARY KEY,
    order_id INT NOT NULL,
    order_item_id INT NOT NULL,
    product_sk INT NOT NULL REFERENCES dim_products(product_sk),
    date_sk INT NOT NULL REFERENCES dim_date(date_sk),
    
    -- Metrics
    quantity INT,
    unit_price DECIMAL(10, 2),
    line_amount DECIMAL(12, 2),
    discount_per_unit DECIMAL(5, 2),
    discount_amount DECIMAL(12, 2),
    net_amount DECIMAL(12, 2),
    
    -- Metadata
    source_timestamp TIMESTAMP,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT pk_facts_order_items PRIMARY KEY (order_item_fact_sk),
    CONSTRAINT uq_order_item_id UNIQUE(order_item_id)
);

CREATE INDEX idx_facts_order_items_product ON facts_order_items(product_sk);
CREATE INDEX idx_facts_order_items_date ON facts_order_items(date_sk);
CREATE INDEX idx_facts_order_items_order ON facts_order_items(order_id);

-- FACT TABLE: FACTS_RETURNS
CREATE TABLE IF NOT EXISTS facts_returns (
    return_fact_sk SERIAL PRIMARY KEY,
    return_id INT NOT NULL,
    order_id INT NOT NULL,
    product_sk INT NOT NULL REFERENCES dim_products(product_sk),
    customer_sk INT NOT NULL REFERENCES dim_customers(customer_sk),
    date_sk INT NOT NULL REFERENCES dim_date(date_sk),
    
    -- Metrics
    refund_amount DECIMAL(10, 2),
    return_reason VARCHAR(500),
    daysn_to_return INT,
    
    -- Metadata
    source_timestamp TIMESTAMP,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT pk_facts_returns PRIMARY KEY (return_fact_sk),
    CONSTRAINT uq_return_id UNIQUE(return_id)
);

CREATE INDEX idx_facts_returns_customer ON facts_returns(customer_sk);
CREATE INDEX idx_facts_returns_product ON facts_returns(product_sk);
CREATE INDEX idx_facts_returns_date ON facts_returns(date_sk);

-- ============================================================================
-- ANALYTICS VIEWS
-- ============================================================================

-- Daily Sales Summary
CREATE OR REPLACE VIEW vw_daily_sales_summary AS
SELECT
    d.date,
    d.year,
    d.month,
    COUNT(DISTINCT fo.order_fact_sk) as total_orders,
    COUNT(DISTINCT fo.customer_sk) as unique_customers,
    SUM(fo.final_amount)::DECIMAL(12, 2) as total_revenue,
    AVG(fo.final_amount)::DECIMAL(12, 2) as avg_order_value,
    SUM(foi.quantity) as total_items,
    SUM(foi.discount_amount)::DECIMAL(12, 2) as total_discounts
FROM dim_date d
LEFT JOIN facts_orders fo ON d.date_sk = fo.date_sk
LEFT JOIN facts_order_items foi ON fo.order_id = foi.order_id
GROUP BY d.date, d.year, d.month;

-- Customer Lifetime Value (CLV)
CREATE OR REPLACE VIEW vw_customer_lifetime_value AS
SELECT
    dc.customer_sk,
    dc.customer_id,
    dc.name,
    COUNT(DISTINCT fo.order_fact_sk) as total_orders,
    SUM(fo.final_amount)::DECIMAL(12, 2) as lifetime_revenue,
    AVG(fo.final_amount)::DECIMAL(12, 2) as avg_order_value,
    SUM(foi.quantity) as total_items_purchased,
    MAX(fo.date_sk) as last_order_date_sk,
    CASE 
        WHEN SUM(fo.final_amount) > 10000 THEN 'VIP'
        WHEN SUM(fo.final_amount) > 5000 THEN 'Premium'
        WHEN SUM(fo final_amount) > 1000 THEN 'Regular'
        ELSE 'New'
    END as customer_segment
FROM dim_customers dc
LEFT JOIN facts_orders fo ON dc.is_current = TRUE AND dc.customer_sk = fo.customer_sk
LEFT JOIN facts_order_items foi ON fo.order_id = foi.order_id
GROUP BY dc.customer_sk, dc.customer_id, dc.name;

-- Product Performance Analysis
CREATE OR REPLACE VIEW vw_product_performance AS
SELECT
    dp.product_sk,
    dp.product_id,
    dp.name,
    dp.category,
    COUNT(DISTINCT foi.order_item_fact_sk) as times_ordered,
    SUM(foi.quantity) as total_units_sold,
    SUM(foi.net_amount)::DECIMAL(12, 2) as total_revenue,
    AVG(foi.unit_price)::DECIMAL(10, 2) as avg_selling_price,
    SUM(foi.discount_amount)::DECIMAL(12, 2) as total_discount_given
FROM dim_products dp
LEFT JOIN facts_order_items foi ON dp.is_current = TRUE AND dp.product_sk = foi.product_sk
GROUP BY dp.product_sk, dp.product_id, dp.name, dp.category;

-- Return Analysis
CREATE OR REPLACE VIEW vw_return_analysis AS
SELECT
    fr.product_sk,
    dp.name as product_name,
    dp.category,
    COUNT(DISTINCT fr.return_fact_sk) as total_returns,
    SUM(fr.refund_amount)::DECIMAL(12, 2) as total_refunded,
    fr.return_reason,
    ROUND(100.0 * COUNT(DISTINCT fr.return_fact_sk) / 
        NULLIF(SUM(COUNT(DISTINCT fr.return_fact_sk)) OVER (PARTITION BY fr.product_sk), 0), 2) as return_percentage
FROM facts_returns fr
LEFT JOIN dim_products dp ON fr.product_sk = dp.product_sk
GROUP BY fr.product_sk, dp.name, dp.category, fr.return_reason;
