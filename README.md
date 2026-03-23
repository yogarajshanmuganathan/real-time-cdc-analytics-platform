# Real-Time Analytics Platform with Change Data Capture (CDC)

**Enterprise-Grade Real-Time Data Warehouse** | PostgreSQL + Kafka + Debezium | SCD Type 2 Dimensions | Event-Driven Architecture


## 🎯 Key Highlights

- ⚡ Real-time CDC pipeline (<1s latency)
- 🔄 Event-driven architecture using Kafka
- 🧠 SCD Type 2 dimensional modeling
- 🏗️ Production-grade data engineering design
---

## 📊 Project Overview

This is a **production-ready** real-time analytics platform that captures changes from a transactional PostgreSQL database using **Debezium (CDC)**, streams them through **Apache Kafka**, and loads them into an analytics warehouse using **SCD Type 2 slowly changing dimensions**.

This project demonstrates **advanced data engineering concepts**:
- ✅ **Change Data Capture (CDC)** - Capture transactional changes without impacting source
- ✅ **Event-Driven Architecture** - Real-time streaming instead of batch processing
- ✅ **Slowly Changing Dimensions (SCD Type 2)** - Track dimension history with effective/end dates
- ✅ **Star Schema Modeling** - Kimball-style dimensional data warehouse
- ✅ **Advanced PL/SQL** - Stored procedures, triggers, CTEs, window functions
- ✅ **Stream Processing** - Kafka consumer with connection pooling
- ✅ **Data Quality** - Error handling, logging, monitoring

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    SOURCE SYSTEM (Production)                   │
│                  PostgreSQL Transactional DB                    │
│  (Customers, Products, Orders, Returns, Inventory)             │
│            WAL (Write-Ahead Log) - Real-time CDC               │
└────────────────────────────┬────────────────────────────────────┘
                             │ CDC Events
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    DEBEZIUM PLATFORM                            │
│             PostgreSQL Logical Decoding (pgoutput)             │
│                    Extract Changes                             │
└────────────────────────────┬────────────────────────────────────┘
                             │ JSON Events
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                   APACHE KAFKA MESSAGE BUS                      │
│  Topics: customers | products | orders | order_items | returns│
│                 (Persistent Event Log)                         │
└────────────────────────────┬────────────────────────────────────┘
                             │ Stream
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│              PYTHON STREAM CONSUMER (Transform)                 │
│  - SCD Type 2 Logic         - Connection Pooling              │
│  - Dimension Lookups        - Error Handling                   │
│  - Fact Table Loading       - Batch Processing                 │
└────────────────────────────┬────────────────────────────────────┘
                             │ Transforms
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│              ANALYTICS WAREHOUSE (PostgreSQL)                   │
│                                                                 │
│  DIMENSIONS:                    FACTS:                         │
│  - dim_customers (SCD Type 2)  - facts_orders                 │
│  - dim_products (SCD Type 2)   - facts_order_items            │
│  - dim_date (Conformed)        - facts_returns                │
│  - dim_order_status                                           │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
                   ┌─────────────────────┐
                   │  ANALYTICS VIEWS    │
                   │  & BI DASHBOARDS    │
                   │  Power BI / Tableau │
                   └─────────────────────┘
```

---

## 🎯 Key Features

### 1. **Change Data Capture (CDC)**
- Real-time extraction of inserts, updates, and deletes
- No impact on source database performance
- Debezium PostgreSQL connector with logical decoding
- Kafka topics for each table

### 2. **SCD Type 2 Dimensions**
- Track full history of customer and product changes
- Effective dates and end dates on dimension records
- `is_current` flag for quick filtered queries
- Enables temporal analysis ("how was this customer when this order happened?")

### 3. **Star Schema Data Warehouse**
- Fact tables (orders, order_items, returns)
- Conformed dimensions (date, order_status)
- Foreign key relationships
- Optimized for analytics queries

### 4. **Advanced PL/SQL**
- Stored procedures for dimension loading (SCD Type 2)
- User-defined functions for analytics
- Triggers for automatic `updated_at` timestamps
- Materialized views for aggregations

### 5. **Real-Time Analytics Views**
- Daily sales summary
- Customer lifetime value (CLV)
- Product performance
- Return analysis
- Monthly trends

### 6. **Stream Processing**
- Kafka consumer with connection pooling
- Parallel event processing
- Error tolerance and logging
- Automatic offset management

---

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- 8GB RAM minimum
- Ports: 5432, 5433, 9092, 8083, 8080

### 1. **Start Infrastructure**
```bash
docker-compose up -d

# Wait for services to be healthy
docker ps
```

**Services Running:**
- PostgreSQL Source (port 5432)
- PostgreSQL Warehouse (port 5433)
- Kafka (port 9092)
- Debezium Kafka Connect (port 8083)
- Kafka UI (port 8080)

### 2. **Deploy CDC Connector**
```bash
# Deploy Debezium connector
bash config/deploy-connector.sh

# Verify connector is running
curl http://localhost:8083/connectors/postgres-cdc-source/status | jq
```

### 3. **Generate Test Data**
```bash
cd python

# Install dependencies
pip install -r requirements.txt

# Generate sample data (100 customers, 150 products, 300 orders)
python data_generator.py
```

### 4. **Start Stream Consumer**
```bash
# Open new terminal
cd python
python cdc_consumer.py

# You should see logs like:
# INFO - Processing customer event: ID=1
# INFO - Processing order event: ID=1, Status=pending
```

### 5. **Run Analytics Queries**
```bash
# In another terminal
python analytics_queries.py
```

---

## 📁 Project Structure

```
real-time-analytics-cdc-platform/
│
├── docker-compose.yml                      # Infrastructure as Code
│
├── config/
│   ├── debezium-source.json               # Debezium connector config
│   └── deploy-connector.sh                # Deployment script
│
├── sql/
│   ├── 01_source_schema.sql              # Source database schema
│   ├── 02_procedures.sql                 # Stored procedures & functions
│   ├── 03_warehouse_schema.sql           # Warehouse dimensions & facts
│   └── 04_scd_type2.sql                  # SCD Type 2 loading procedures
│
├── python/
│   ├── cdc_consumer.py                   # Kafka → Warehouse consumer
│   ├── data_generator.py                 # Synthetic data generation
│   ├── analytics_queries.py              # BI reporting queries
│   └── requirements.txt                  # Python dependencies
│
├── docs/
│   ├── ARCHITECTURE.md                   # Detailed architecture
│   ├── SETUP_GUIDE.md                    # Step-by-step setup
│   └── DATA_LINEAGE.md                   # Data flow documentation
│
└── README.md
```

---

## 📚 Core Concepts

### CDC (Change Data Capture)
**Why?** Instead of batch queries or full table scans, Debezium reads from the PostgreSQL WAL (Write-Ahead Log) to capture every change in real-time.

**How?** 
1. PostgreSQL writes all changes to WAL for durability
2. Debezium reads WAL using logical decoding
3. Changes converted to JSON messages
4. Published to Kafka topics

**Benefit:** <1 second latency vs. 24-hour batch jobs

### SCD Type 2 (Slowly Changing Dimensions)
Tracks historical versions of dimension attributes:

```sql
-- Customer moves to a new city
-- Instead of updating the record, we:
1. Close old version: end_date = NOW(), is_current = FALSE
2. Insert new version: effective_date = NOW(), is_current = TRUE
3. Analysis queries join current version only
4. Historical queries can see all versions
```

**Example:**
```sql
-- "Show orders from customers who were based in NY at order time"
SELECT o.order_id, c.name, c.city
FROM facts_orders o
JOIN dim_customers c ON o.customer_sk = c.customer_sk
WHERE c.city = 'New York' 
  AND c.effective_date <= o.date_sk
  AND (c.end_date IS NULL OR c.end_date > o.date_sk);
```

### Star Schema
Separates business logic into:
- **Dimensions:** WHO, WHAT, WHEN (customers, products, dates)
- **Facts:** Measurements (orders, revenue, quantities)

```
                    dim_customers
                           │
                           │ customer_sk
                           ▼
fact_orders ◄──────────────╋────────────► dim_date
(metrics)                  │
                           │ product_sk
                           ▼
                    dim_products
```

---

## 🔄 Data Flow: Order Creation Example

### Source System (PostgreSQL Source)
```sql
INSERT INTO orders (customer_id, order_date, total_amount, status)
VALUES (42, NOW(), 249.99, 'pending');
```

### CDC Capture (Debezium)
```json
{
  "op": "c",
  "ts_ms": 1679000000000,
  "after": {
    "order_id": 12345,
    "customer_id": 42,
    "order_date": "2024-03-23T10:30:00Z",
    "total_amount": 249.99,
    "status": "pending"
  }
}
```

### Kafka Topic
```
Topic: orders
Partition 0: [CDC event] → [CDC event] → [CDC event]
```

### Stream Consumer (Python)
```python
event = consumer.poll()  # Gets CDP event from Kafka
# Calls: sp_load_facts_orders(12345, 42, '2024-03-23', 'pending', ...)
# Executes PL/SQL to:
#   1. Lookup customer_sk in dim_customers
#   2. Lookup date_sk in dim_date
#   3. Lookup status_sk in dim_order_status
#   4. Insert into facts_orders
```

### Analytics Warehouse
```sql
SELECT 
  o.order_id,
  c.name,           -- from dim_customers
  c.city,           -- current city at query time
  p.name,           -- from dim_products
  SUM(oi.quantity)  -- aggregated facts
FROM facts_orders o
JOIN dim_customers c ON o.customer_sk = c.customer_sk AND c.is_current = TRUE
JOIN dim_order_status s ON o.status_sk = s.status_sk
JOIN facts_order_items oi ON o.order_id = oi.order_id
...
```

---

## 🎓 Advanced SQL Patterns Used

### 1. **SCD Type 2 Implementation**
```sql
CREATE OR REPLACE PROCEDURE sp_load_dim_customers(
    p_customer_id INT,
    p_name VARCHAR,
    ...
) LANGUAGE plpgsql AS $$
DECLARE
    v_current_sk INT;
    v_changed BOOLEAN := FALSE;
BEGIN
    -- Find current record
    SELECT customer_sk INTO v_current_sk
    FROM dim_customers
    WHERE customer_id = p_customer_id AND is_current = TRUE;

    IF v_current_sk IS NULL THEN
        -- INSERT new dimension
    ELSE
        -- CHECK if changed
        SELECT EXISTS (
            SELECT 1 FROM dim_customers
            WHERE customer_sk = v_current_sk
            AND (name IS DISTINCT FROM p_name OR ...)
        ) INTO v_changed;

        IF v_changed THEN
            -- CLOSE old version
            UPDATE dim_customers
            SET end_date = CURRENT_TIMESTAMP, is_current = FALSE
            WHERE customer_sk = v_current_sk;

            -- INSERT new version (SCD Type 2)
            INSERT INTO dim_customers (...)
            VALUES (...);
        END IF;
    END IF;
END;
$$;
```

### 2. **Complex Analytics Query (Window Functions + CTEs)**
```sql
WITH customer_segments AS (
    SELECT
        customer_sk,
        SUM(final_amount) OVER (PARTITION BY customer_sk) as ltv,
        RANK() OVER (ORDER BY SUM(final_amount) DESC) as customer_rank
    FROM facts_orders
)
SELECT
    CASE 
        WHEN customer_rank <= 100 THEN 'VIP'
        WHEN customer_rank <= 1000 THEN 'Premium'
        ELSE 'Regular'
    END as segment,
    ...
FROM customer_segments;
```

### 3. **Materialized View with Automatic Refresh**
```sql
CREATE MATERIALIZED VIEW mv_daily_order_summary AS
SELECT
    DATE(o.order_date) as order_date,
    o.status,
    COUNT(*) as total_orders,
    SUM(o.total_amount) as total_revenue
FROM orders o
GROUP BY DATE(o.order_date), o.status;

-- Refresh after CDC load:
REFRESH MATERIALIZED VIEW mv_daily_order_summary;
```

---

## 🔍 Monitoring & Troubleshooting

### 1. **Check CDC Connector Status**
```bash
curl http://localhost:8083/connectors/postgres-cdc-source/status | jq

# Expected response:
# {
#   "name": "postgres-cdc-source",
#   "connector": { "state": "RUNNING" },
#   "tasks": [{ "state": "RUNNING", "id": 0 }]
# }
```

### 2. **Monitor Kafka Topics**
```bash
# List topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Check topic lag
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group cdc-consumer-group \
  --describe

# View messages
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic customers \
  --from-beginning
```

### 3. **Check Consumer Logs**
```bash
# Monitor consumer processing
tail -f python/consumer.log

# Look for errors
grep ERROR python/consumer.log
```

### 4. **Warehouse Data Validation**
```bash
# Connect to warehouse
psql -h localhost -p 5433 -U warehouse_user -d analytics_warehouse

# Check dimension load status
SELECT COUNT(*) FROM dim_customers WHERE is_current = TRUE;
SELECT COUNT(*) FROM facts_orders;

-- Check SCD Type 2 history
SELECT customer_id, name, city, effective_date, end_date, is_current
FROM dim_customers
WHERE customer_id = 42
ORDER BY effective_date DESC;
```

---

## 📊 Key SQL Queries

### Customer Lifetime Value (CLV)
```sql
SELECT
    dc.customer_id,
    dc.name,
    COUNT(DISTINCT fo.order_fact_sk) as total_orders,
    SUM(fo.final_amount)::DECIMAL(12, 2) as lifetime_revenue,
    AVG(fo.final_amount)::DECIMAL(12, 2) as avg_order_value,
    CASE 
        WHEN SUM(fo.final_amount) > 10000 THEN 'VIP'
        WHEN SUM(fo.final_amount) > 5000 THEN 'Premium'
        ELSE 'Regular'
    END as segment
FROM dim_customers dc
LEFT JOIN facts_orders fo ON dc.is_current = TRUE AND dc.customer_sk = fo.customer_sk
GROUP BY dc.customer_sk, dc.customer_id, dc.name
ORDER BY lifetime_revenue DESC;
```

### Product Performance with Returns
```sql
SELECT
    dp.name,
    dp.category,
    COUNT(DISTINCT foi.order_item_fact_sk) as times_ordered,
    SUM(foi.quantity) as total_units_sold,
    SUM(foi.net_amount)::DECIMAL(12, 2) as total_revenue,
    COUNT(DISTINCT fr.return_fact_sk) as total_returns,
    ROUND(100.0 * COUNT(DISTINCT fr.return_fact_sk) / NULLIF(COUNT(DISTINCT foi.order_item_fact_sk), 0), 2) as return_rate_pct
FROM dim_products dp
LEFT JOIN facts_order_items foi ON dp.is_current = TRUE AND dp.product_sk = foi.product_sk
LEFT JOIN facts_returns fr ON dp.product_sk = fr.product_sk
GROUP BY dp.product_sk, dp.name, dp.category
HAVING COUNT(DISTINCT foi.order_item_fact_sk) > 0
ORDER BY total_revenue DESC;
```

### Monthly Sales Trend
```sql
SELECT
    MAKE_DATE(dd.year, dd.month, 1) as period,
    COUNT(DISTINCT fo.order_fact_sk) as order_count,
    SUM(fo.final_amount)::DECIMAL(12, 2) as monthly_revenue,
    ROUND(AVG(fo.final_amount), 2)::DECIMAL(12, 2) as aov,
    COUNT(DISTINCT fo.customer_sk) as unique_customers
FROM facts_orders fo
JOIN dim_date dd ON fo.date_sk = dd.date_sk
GROUP BY MAKE_DATE(dd.year, dd.month, 1)
ORDER BY period DESC;
```

---

## 🎓 Learning Outcomes

By completing this project, you'll gain expertise in:

✅ **CDC Architecture**
- PostgreSQL logical decoding
- Debezium platform and connectors
- Event sourcing patterns

✅ **Advanced PL/SQL**
- Stored procedures with parameters
- SCD Type 2 logic implementation
- Window functions and CTEs
- Triggers for automation
- Performance optimization

✅ **Data Warehouse Design**
- Kimball dimensional modeling
- Star schema creation
- Conformed dimensions
- Fact table design

✅ **Stream Processing**
- Kafka architecture and topics
- Consumer group management
- Connection pooling
- Error handling

✅ **Event-Driven Patterns**
- Real-time data pipelines
- Transactional consistency
- Latency optimization

✅ **Python Data Engineering**
- Kafka consumer implementation
- Database connection management
- Batch processing patterns

---

## 🔧 Configuration & Tuning

### Debezium Performance
```json
{
  "max.batch.size": 2048,           // Increase for higher throughput
  "max.queue.size": 8192,           // Buffer size
  "poll.interval.ms": 1000,         // WAL poll frequency
  "heartbeat.interval.ms": 10000    // Keep-alive signal
}
```

### Consumer Performance
```python
KafkaConsumer(
    bootstrap_servers=['kafka:9092'],
    max_poll_records=100,            # Batch size
    session_timeout_ms=30000,        # Heartbeat interval
    fetch_min_bytes=1024 * 10,       // Batch threshold
)
```

### Database Indexing
```sql
-- Key indices for performance
CREATE INDEX idx_dim_customers_current ON dim_customers(is_current) WHERE is_current = TRUE;
CREATE INDEX idx_facts_orders_customer_date ON facts_orders(customer_sk, date_sk);
CREATE INDEX idx_dim_customers_id ON dim_customers(customer_id);
```

---

## 🚨 Troubleshooting Common Issues

### Issue: Connector Won't Connect
```bash
# Check PostgreSQL WAL config
docker exec postgres-source psql -U source_user -d transactional_db -c \
  "SELECT wal_level, max_wal_senders, max_replication_slots FROM pg_settings;"

# Should show: wal_level=logical, max_wal_senders>=10
```

### Issue: Consumer Lagging
```python
# Check lag in consumer group
consumer.committed()  # Get current offset
consumer.committed(partition)  # Per partition

# Increase batch size to catch up faster
consumer = KafkaConsumer(max_poll_records=500)
```

### Issue: Warehouse Queries Slow
```sql
-- Analyze and vacuum
ANALYZE dim_customers;
VACUUM ANALYZE facts_orders;

-- Check explain plan
EXPLAIN ANALYZE SELECT ...;

-- Add missing indices
CREATE INDEX idx_orders_date ON facts_orders(date_sk) WHERE date_sk > 20240101;
```

---

## 📈 Scaling Considerations

1. **Kafka Partitioning:** Partition by customer_id for customer-scoped analytics
2. **Connection Pooling:** Increase pool size as consumer volume grows
3. **Warehouse Sharding:** Consider table partitioning for facts_orders by date
4. **CDC Replication Slots:** Monitor slot lag to prevent WAL buildup
5. **Batch Window:** Tune consumer batch timing vs. latency requirements

---

## 🎯 Next Steps & Extensions

1. **Add Data Quality Checks**
   - Great Expectations for data validation
   - dbt tests for fact/dimension integrity

2. **Implement Real-Time Dashboards**
   - Metabase/Superset for visualization
   - WebSocket updates for live data

3. **Advanced Transformations**
   - Spark Streaming for complex aggregations
   - ML models for anomaly detection

4. **High Availability**
   - Kafka cluster with replication
   - PostgreSQL streaming replication
   - Consumer group failover

5. **Compliance & Security**
   - Encryption at rest and in transit
   - Row-level security (RLS)
   - Audit logging of changes

---

## 📞 Support & Resources

- **Debezium Docs:** https://debezium.io/documentation/
- **Kafka Docs:** https://kafka.apache.org/documentation/
- **PostgreSQL CDC:** https://www.postgresql.org/docs/current/logicaldecoding.html
- **Kimball Methodology:** https://www.kimballgroup.com/

---

## ✨ Summary

This project demonstrates **enterprise-grade data engineering** combining:
- **Real-time CDC** for zero-latency data integration
- **Advanced SQL** for complex transformations
- **Stream processing** for event-driven pipelines
- **Dimensional modeling** for business analytics

You're now equipped to build **production data pipelines** that scale to millions of events per day!

---

**Built with:** PostgreSQL • Kafka • Debezium • Python • Docker

**Last Updated:** March 23, 2026
