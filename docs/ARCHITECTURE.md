# Architecture Deep Dive

## System Components

### 1. PostgreSQL Source (Transactional)
**Role:** Operational database for e-commerce transactions

**Schema Tables:**
- `customers` - User accounts with profile info
- `products` - Catalog with inventory
- `orders` - Order headers with status
- `order_items` - Line items detail
- `returns` - Return management
- `inventory_movements` - Stock tracking

**CDC Configuration:**
```
wal_level = logical              # Enable logical decoding
max_wal_senders = 10             # Allow replication connections
max_replication_slots = 10       # CDC reader slots
publication_name = cdc_pub       # Publication for tables
```

**Logical Decoding:** PostgreSQL uses WAL (Write-Ahead Log) in logical format:
- Every INSERT, UPDATE, DELETE is logged
- Debezium reads these logs in real-time
- No impact on application performance

### 2. Debezium Kafka Connect
**Role:** CDC platform that reads from source and publishes to Kafka

**Architecture:**
```
PostgreSQL WAL
    ↓ (pgoutput plugin)
Debezium PostgreSQL Connector
    ↓ (Extract & Transform)
JSON Messages
    ↓
Kafka Topics
```

**Connector Configuration:**
- `plugin.name = pgoutput` - PostgreSQL's native logical decoding plugin
- `publication.name = cdc_pub` - Only published tables
- `table.include.list = public.orders,...` - Filter tables
- `transformations` - Extract new record state, route by table

**Key Features:**
- **Exactly-once semantics** - Messages not duplicated
- **Heartbeat** - Keeps connection alive
- **Snapshot mode** - Initial full load of existing data
- **Incremental mode** - Only changes after snapshot

### 3. Apache Kafka Message Bus
**Role:** Distributed event log and buffer

**Topics Created (One per Table):**
```
Topic: orders
├─ Partition 0: [Event 1] → [Event 2] → [Event 3]
├─ Partition 1: [Event 4] → [Event 5] → [Event 6]
└─ Partition 2: [Event 7] → [Event 8] → [Event 9]
```

**Message Format:**
```json
{
  "schema": {
    "type": "struct",
    "fields": [
      {"name": "order_id", "type": "int32"},
      {"name": "customer_id", "type": "int32"},
      ...
    ]
  },
  "payload": {
    "order_id": 12345,
    "customer_id": 42,
    "order_date": "2024-03-23T10:30:00Z",
    "total_amount": 249.99
  }
}
```

**Benefits:**
- **Decoupling** - Producer and consumer independent
- **Buffering** - Handles burst traffic
- **Persistence** - Messages retained for replay
- **Ordering** - Partition-level order guarantee

### 4. Python Stream Consumer
**Role:** Transform CDC events and load into warehouse

**Processing Pipeline:**
```
Kafka Consumer
    ↓ poll()
JSON Event
    ↓ parse
Route to processor (customers, orders, etc.)
    ↓ enrich
Lookup dimension keys (customer_sk, product_sk)
    ↓ transform
Call warehouse stored procedures
    ↓ load
PostgreSQL Warehouse
```

**Performance Features:**
- **Connection Pooling** - Reuse database connections
- **Batch Processing** - Poll 100 records at once
- **Error Handling** - Continue on timeouts/errors
- **Offset Management** - Automatic commit for recovery

**Thread Safety:**
```python
# Connection pool prevents connection exhaustion
pool = SimpleConnectionPool(5, 20, ...)
conn = pool.getconn()
try:
    # Use connection
finally:
    pool.putconn(conn)  # Return to pool
```

### 5. PostgreSQL Analytics Warehouse
**Role:** Dimensional data warehouse for BI

**Schema Architecture:**
```
DIMENSIONS                          FACTS
├─ dim_customers (SCD Type 2)      ├─ facts_orders
├─ dim_products (SCD Type 2)        ├─ facts_order_items
├─ dim_date (Conformed)             └─ facts_returns
└─ dim_order_status
```

---

## Data Flow: Detailed Example

### Scenario: Customer Updates Email

**Step 1: Source Database**
```sql
-- User updates profile
UPDATE customers 
SET email = 'newemail@example.com'
WHERE customer_id = 42;
```

**Step 2: PostgreSQL WAL**
```
WAL Entry: UPDATE customers SET email='newemail@example.com' WHERE customer_id=42
Timestamp: 2024-03-23 10:30:45.123
Transaction ID: 12345
```

**Step 3: Debezium Reads WAL**
- Scans logical decoding slot `cdc_slot`
- Converts to logical change record
- Filters by publication `cdc_pub`
- Extracts "before" and "after" states

**Step 4: Kafka Message**
```json
{
  "before": {
    "customer_id": 42,
    "name": "John Smith",
    "email": "oldemail@example.com"
  },
  "after": {
    "customer_id": 42,
    "name": "John Smith",
    "email": "newemail@example.com"
  },
  "op": "u",
  "ts_ms": 1679000000123
}
```

**Step 5: Kafka Topic (customers)**
```
Message published to partition based on customer_id
Retention: 7 days (default)
Replication factor: 1 (can be increased)
```

**Step 6: Consumer Polls**
```python
messages = consumer.poll(timeout_ms=1000, max_records=100)
# Gets batch of 100 messages from multiple partitions
```

**Step 7: Stream Consumer Processing**
```python
def _process_customer_event(self, event_data):
    # Call stored procedure with SCD Type 2 logic
    cur.execute("""
        CALL sp_load_dim_customers(%s,%s,...,%s)
    """, (customer_id, name, newemail, ...))
```

**Step 8: Warehouse Procedure (SCD Type 2)**
```sql
PROCEDURE sp_load_dim_customers:
1. SELECT current record: customer_id=42, is_current=TRUE
2. Compare new values:
   - name: SAME
   - email: DIFFERENT
3. Changed detected!
4. UPDATE old record: end_date=NOW(), is_current=FALSE
5. INSERT new record: 
   - email=newemail@example.com
   - effective_date=NOW()
   - is_current=TRUE

Result:
customer_sk | customer_id | email                  | effective_date | end_date | is_current
101         | 42          | oldemail@example.com   | 2024-03-20     | 2024-03-23 | FALSE
102         | 42          | newemail@example.com   | 2024-03-23     | NULL       | TRUE
```

**Step 9: BI Query for Temporal Analysis**
```sql
-- "Show all orders from customers at their email at order time"
SELECT 
    o.order_id,
    dc.customer_id,
    dc.email    -- Email at time of order
FROM facts_orders o
JOIN dim_customers dc ON o.customer_sk = dc.customer_sk
WHERE o.order_date >= dc.effective_date
  AND (o.order_date < dc.end_date OR dc.is_current = TRUE);
```

---

## SCD Type 2 State Machine

```
                    ┌─────────────────────┐
                    │  New Customer       │
                    │  INSERT Record      │
                    │  is_current=T       │
                    └──────────┬──────────┘
                               │
                               │ Customer info changed
                               ▼
                    ┌─────────────────────┐
                    │ Old Version         │◄────┐
                    │ UPDATE              │     │
                    │ end_date=NOW()      │     │
                    │ is_current=FALSE    │     │
                    └─────────────────────┘     │
                               │                │
                               │                │ Update repeats
                               ▼                │
                    ┌─────────────────────┐     │
                    │ New Version         │─────┘
                    │ INSERT new record   │
                    │ effective_date=NOW()│
                    │ is_current=TRUE     │
                    └─────────────────────┘
```

---

## Performance Characteristics

### Query Performance by Operation

| Operation | Source | Latency | Impact |
|-----------|--------|---------|--------|
| Insert 1000 orders | Batch job | Hours | High (locks) |
| Insert 1000 orders (CDC) | Real-time | <1 sec/event | Minimal |
| Daily aggregation | Batch | 10 minutes | Blocks queries |
| Real-time metric | View | <100ms | None |

### Scalability Limits

| Component | Limit | Solution |
|-----------|-------|----------|
| Kafka partition | 1M msgs/sec | Add partitions |
| Consumer throughput | 10K msgs/sec | Add consumers |
| PostgreSQL connections | 100 default | Increase in pg.conf |
| Dimension size | 100M rows | Partition by date |

---

## Error Handling Strategy

### Consumer Resilience

```python
try:
    message = consumer.poll()
    process_event(message)
except KafkaError as e:
    # Network error - retry exponential backoff
    logger.error(f"Kafka error: {e}")
    backoff(2)  # 2 seconds
except psycopg2.Error as e:
    # Database error - log and continue
    logger.error(f"Database error: {e}")
    error_count += 1  # Monitor
finally:
    consumer.commit()  # Commit offset regardless
```

### Replayability

```
If consumer crashes at event 1000:
1. Restart consumer
2. Kafka tracks committed offset (999)
3. Consumer resumes at event 1000
4. No data loss
5. Potential duplicates handled by idempotent upserts
```

---

## Monitoring Metrics

### Key Metrics to Track

```sql
-- Consumer lag (how far behind are we?)
SELECT 
    topic,
    PARTITION,
    lag,  -- Kafka consumer lag
    member_id
FROM kafka_consumer_groups;

-- Warehouse load latency (source → warehouse)
SELECT 
    AVG(EXTRACT(EPOCH FROM (load_timestamp - source_timestamp))) as avg_latency_seconds
FROM facts_orders;

-- Dimension change velocity
SELECT 
    COUNT(*) / 24 as hourly_changes_per_dim
FROM dim_customers
WHERE effective_date > NOW() - INTERVAL '1 hour';
```

### Alert Thresholds

- **Consumer lag > 10 minutes** → Page on-call
- **Load latency > 5 seconds** → Investigate
- **Error rate > 1%** → Page on-call
- **Kafka replication lag > 0** → Check broker health

---

## Disaster Recovery

### Snapshot Recovery

```bash
# Export warehouse
pg_dump analytics_warehouse > warehouse_backup.sql

# Export CDC state
# (Kafka automatically retains 7 days)

# If warehouse corrupted:
DROP DATABASE analytics_warehouse;
CREATE DATABASE analytics_warehouse;
psql analytics_warehouse < warehouse_backup.sql
# Consumer resumes from saved offset
```

### Point-in-Time Recovery

Because we track `effective_date` and maintain history:

```sql
-- Recover customer data as of March 15
SELECT *
FROM dim_customers
WHERE effective_date <= '2024-03-15'
  AND (end_date IS NULL OR end_date > '2024-03-15');
```

---

This architecture provides:
✅ Real-time data integration  
✅ Historical tracking (SCD Type 2)  
✅ Scalable event processing  
✅ Point-in-time analysis  
✅ Disaster recovery capability  
