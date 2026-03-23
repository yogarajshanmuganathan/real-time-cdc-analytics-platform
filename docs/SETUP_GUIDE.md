# Setup Guide - Step by Step

Complete guide to get the CDC platform running locally.

## Prerequisites

- **Docker Desktop** (v20.10+ with compose v2)
- **Git**
- **Python 3.9+**
- **psql** CLI (for testing)
- **jq** (for JSON parsing - optional)

**System Requirements:**
- 8GB RAM minimum
- 10GB disk space
- Ports 5432, 5433, 9092, 8083, 8080 available

## Installation

### 1. Clone and Navigate
```bash
cd c:\DataEngineering\01_projects\real-time-analytics-cdc-platform
```

### 2. Start Docker Containers
```bash
docker-compose up -d

# Verify all services are running
docker ps

# Expected output:
# postgres-source       postgres:15-alpine     Up 2 minutes
# postgres-warehouse    postgres:15-alpine     Up 2 minutes
# zookeeper             cp-zookeeper:7.5       Up 2 minutes
# kafka                 cp-kafka:7.5.0         Up 2 minutes
# kafka-connect         debezium/connect:2.4   Up 1 minute
# kafka-ui              kafka-ui:latest        Up 1 minute
```

### 3. Wait for Healthy Status
```bash
# Check service health
docker ps --format "table {{.Names}}\t{{.Status}}"

# Wait for all "healthy" statuses
docker compose logs postgres-source | grep "database system is ready"
docker compose logs postgres-warehouse | grep "database system is ready"
```

### 4. Deploy Debezium Connector
```bash
# Make script executable
chmod +x config/deploy-connector.sh

# Deploy connector
bash config/deploy-connector.sh

# Verify deployment
curl http://localhost:8083/connectors/postgres-cdc-source/status | jq

# Expected:
# {
#   "name": "postgres-cdc-source",
#   "connector": { "state": "RUNNING" },
#   "tasks": [{ "state": "RUNNING", "id": 0 }]
# }
```

### 5. Verify Kafka Topics Created
```bash
# List topics
docker exec kafka kafka-topics \
  --list \
  --bootstrap-server localhost:9092

# Expected topics:
# customers
# products
# orders
# order_items
# returns
# inventory_movements
```

### 6. Set Up Python Environment
```bash
cd python

# Create virtual environment
python -m venv venv

# Activate virtual environment
# Windows:
venv\Scripts\activate
# macOS/Linux:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Verify installation
pip list
```

### 7. Generate Initial Data
```bash
# Generate sample data
python data_generator.py

# Expected output:
# INFO - Generating 100 customers, 150 products, 300 orders...
# INFO - ✓ Generated 100 customers
# INFO - ✓ Generated 150 products
# INFO - ✓ Generated 300 orders
# INFO - Data generation complete
```

### 8. Verify Data in Source Database
```bash
# Connect to source
psql -h localhost -p 5432 -U source_user -d transactional_db

# Run verification queries
transactional_db=> SELECT COUNT(*) as customer_count FROM customers;
 customer_count
---------------
            100
(1 row)

transactional_db=> SELECT COUNT(*) as order_count FROM orders;
 order_count
-----------
        300
(1 row)

transactional_db=> \q  # Exit psql
```

### 9. Monitor Kafka Messages (Optional)
```bash
# In a terminal, watch Kafka topics
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --from-beginning \
  --max-messages 5 | jq

# You should see JSON CDC events like:
# {
#   "after": {
#     "order_id": 1,
#     "customer_id": 42,
#     "total_amount": "249.99",
#     ...
#   },
#   "op": "c",
#   "ts_ms": 1679000000000
# }
```

### 10. Start Stream Consumer
```bash
# Start consuming CDC events and loading to warehouse
python cdc_consumer.py

# Expected output:
# INFO - Starting CDC consumer for topics: [...]
# INFO - Processed customer event: ID=1
# INFO - Processed order event: ID=1, Status=pending
# INFO - Progress: 100 events processed, 0 errors
# INFO - Progress: 200 events processed, 0 errors
```

### 11. Verify Warehouse Data
```bash
# In another terminal
psql -h localhost -p 5433 -U warehouse_user -d analytics_warehouse

# Check dimensions loaded
analytics_warehouse=> SELECT COUNT(*) as dim_customer_count FROM dim_customers;
 dim_customer_count
------------------
               100
(1 row)

analytics_warehouse=> SELECT COUNT(*) as fact_orders_count FROM facts_orders;
 fact_orders_count
------------------
               300
(1 row)

# Check SCD Type 2 records
analytics_warehouse=> SELECT COUNT(*) FROM dim_customers WHERE is_current = TRUE;
 count
-------
   100
(1 row)

analytics_warehouse=> \q
```

### 12. Run Analytics Queries
```bash
# In another terminal
python analytics_queries.py

# Sample output:
# ================================================================================
# DAILY SALES SUMMARY (LAST 30 DAYS)
# ================================================================================
#
# date                 | year                 | month                | total_orders        
# ---------------
# 2024-03-23           | 2024                 | 3                    | 25
# 2024-03-22           | 2024                 | 3                    | 18
# ...
```

---

## Verification Checklist

- [ ] All Docker containers running (`docker ps` shows 6 services)
- [ ] PostgreSQL source healthy (psql connects on 5432)
- [ ] PostgreSQL warehouse healthy (psql connects on 5433)
- [ ] Kafka broker responsive (`kafka-topics --list` works)
- [ ] Debezium connector RUNNING status
- [ ] Kafka topics exist (6 topics created)
- [ ] Source data loaded (100 customers, 300 orders)
- [ ] Warehouse dimensions loaded
- [ ] Warehouse facts loaded
- [ ] Consumer running without errors
- [ ] Analytics queries return data

---

## Testing Real-Time Changes

### Test 1: New Order CDC
```bash
# Terminal 1: Watch Kafka order topic
docker exec kafka kafka-console-consumer \
  --bootstrap-stream localhost:9092 \
  --topic orders \
  --max-messages 1 | jq

# Terminal 2: Generate new order in source
psql -h localhost -p 5432 -U source_user -d transactional_db
transactional_db=> 
  INSERT INTO orders (customer_id, order_date, total_amount, status)
  VALUES (1, NOW(), 299.99, 'pending');

# Terminal 3: Check consumer logs
# You should see "Processed order event: ID=<new_id>, Status=pending"

# Terminal 4: Verify in warehouse
psql -h localhost -p 5433 -U warehouse_user -d analytics_warehouse
analytics_warehouse=>
  SELECT * FROM facts_orders WHERE order_id = (SELECT MAX(order_id) FROM facts_orders);
```

### Test 2: Customer Update (SCD Type 2)
```bash
# Terminal 1: Watch Kafka customer topic
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic customers \
  --max-messages 1 | jq

# Terminal 2: Update customer in source
psql -h localhost -p 5432 -U source_user -d transactional_db
transactional_db=>
  UPDATE customers SET city = 'Los Angeles' WHERE customer_id = 1;

# Terminal 3: Check warehouse (SCD Type 2)
psql -h localhost -p 5433 -U warehouse_user -d analytics_warehouse
analytics_warehouse=>
  SELECT customer_id, city, effective_date, is_current
  FROM dim_customers
  WHERE customer_id = 1
  ORDER BY effective_date DESC;
  
# Expected: 2 rows - old with end_date, new with is_current=TRUE
```

### Test 3: Return Processing
```bash
# In terminal with psql to source database
transactional_db=>
  -- Get an order to return
  SELECT order_id, customer_id FROM orders LIMIT 1;  -- Returns order_id=100
  
  -- Process return
  CALL process_return(100, (SELECT product_id FROM order_items WHERE order_id=100 LIMIT 1), 'Damaged item');

# In terminal with consumer, watch for:
# INFO - Processed return event: ID=<return_id>

# Verify in warehouse
analytics_warehouse=>
  SELECT * FROM facts_returns ORDER BY load_timestamp DESC LIMIT 1;
```

---

## Common Issues & Solutions

### Issue: Ports Already in Use
```bash
# Find process using port 5432
lsof -i :5432

# Kill process or use different port
# Edit docker-compose.yml and change ports
```

### Issue: Connector Won't Start
```bash
# Check Kafka Connect logs
docker logs kafka-connect

# Common cause: PostgreSQL not ready
docker logs postgres-source | head -20

# Solution: Wait longer for services to be healthy
docker-compose ps
```

### Issue: No Data in Warehouse
```bash
# Check consumer is running
ps aux | grep cdc_consumer.py

# Check for errors
tail -100 python/consumer.log

# Verify source data exists
psql -h localhost -p 5432 -U source_user -d transactional_db
SELECT COUNT(*) FROM orders;

# Verify Kafka messages exist
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --from-beginning \
  --max-messages 1
```

### Issue: Consumer Crashes
```bash
# Get error details
# Look for stack traces in logs
grep ERROR python/consumer.log

# Common cause: Database connection issues
# Check warehouse is healthy
psql -h localhost -p 5433 -U warehouse_user -d analytics_warehouse

# Restart consumer with verbose logging
python cdc_consumer.py --debug 2>&1 | tee consumer.log
```

### Issue: High Consumer Lag
```bash
# Check consumer group status
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group cdc-consumer-group \
  --describe

# LAG column shows events behind
# If > 1000, consumer can't keep up

# Solutions:
# 1. Increase batch size in cdc_consumer.py
# 2. Add more consumer threads
# 3. Reduce other loads on warehouse
```

---

## Cleanup

### Stop Services
```bash
docker-compose down

# Remove volumes (CAUTION - deletes data)
docker-compose down -v
```

### Clean Python Environment
```bash
deactivate  # Exit virtual environment
rm -rf venv
```

---

## Next Steps After Setup

1. **Monitor the system:**
   - Open Kafka UI: http://localhost:8080
   - Check consumer is running: `docker logs kafka-connect`

2. **Run sample analytics:**
   - Execute queries in `python/analytics_queries.py`
   - Create your own analytical queries

3. **Generate continuous load:**
   - Modify `data_generator.py` to create ongoing transactions
   - Watch real-time updates flow to warehouse

4. **Experiment with SCD Type 2:**
   - Update customer data frequently
   - Observe dimension versioning
   - Run temporal queries

5. **Extend the project:**
   - Add more tables to CDC
   - Implement custom transformations
   - Build dashboards

---

## Performance Testing

### Throughput Test
```bash
# Generate high-volume orders
python data_generator.py --orders-per-second 1000 --duration 60

# Monitor consumer
# Watch LAG metric stay < 5 seconds
```

### Latency Test
```bash
# Measure CDC end-to-end latency
# 1. Insert order in source (note timestamp)
# 2. Query warehouse for order (note timestamp)
# 3. Difference = latency

# Target: < 500ms P99
```

---

## Documentation References

- [ARCHITECTURE.md](./ARCHITECTURE.md) - System design deep dive
- [DATA_LINEAGE.md](./DATA_LINEAGE.md) - Data flow documentation
- [../README.md](../README.md) - Project overview

---

**Setup Complete!** 🎉

Your CDC platform is ready for real-time analytics.
