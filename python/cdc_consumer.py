"""
Real-Time CDC Stream Consumer
Consumes CDC events from Kafka and loads into analytics warehouse
"""

import json
import logging
import sys
from datetime import datetime
from typing import Dict, Any, Optional
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from psycopg2 import sql

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CDCStreamConsumer:
    """
    Consumes PostgreSQL CDC events from Kafka and transforms them
    into analytics warehouse using SCD Type 2 logic
    """

    def __init__(
        self,
        kafka_brokers: list,
        kafka_topics: list,
        source_db_config: dict,
        warehouse_db_config: dict,
        group_id: str = 'cdc-consumer-group'
    ):
        self.kafka_brokers = kafka_brokers
        self.kafka_topics = kafka_topics
        self.group_id = group_id

        # Kafka Consumer
        self.consumer = KafkaConsumer(
            *kafka_topics,
            bootstrap_servers=kafka_brokers,
            group_id=group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            error_callback=self._error_callback,
            max_poll_records=100
        )

        # Database connection pools
        self.source_pool = SimpleConnectionPool(
            5, 20,
            host=source_db_config['host'],
            port=source_db_config['port'],
            database=source_db_config['database'],
            user=source_db_config['user'],
            password=source_db_config['password']
        )

        self.warehouse_pool = SimpleConnectionPool(
            5, 20,
            host=warehouse_db_config['host'],
            port=warehouse_db_config['port'],
            database=warehouse_db_config['database'],
            user=warehouse_db_config['user'],
            password=warehouse_db_config['password']
        )

        # Event processors by table
        self.event_processors = {
            'customers': self._process_customer_event,
            'products': self._process_product_event,
            'orders': self._process_order_event,
            'order_items': self._process_order_item_event,
            'returns': self._process_return_event,
            'inventory_movements': self._process_inventory_event
        }

        self.processed_events = 0
        self.error_count = 0

    def _error_callback(self, exc):
        """Handle Kafka errors"""
        logger.error(f"Kafka error: {exc}", exc_info=True)

    def _get_warehouse_connection(self):
        """Get connection from warehouse pool"""
        return self.warehouse_pool.getconn()

    def _put_warehouse_connection(self, conn):
        """Return connection to warehouse pool"""
        self.warehouse_pool.putconn(conn)

    def _extract_table_name(self, topic: str) -> str:
        """Extract table name from Kafka topic"""
        # Topic format: cdc.postgres.source.<table_name>
        parts = topic.split('.')
        return parts[-1] if parts else topic

    def _process_customer_event(self, event_data: Dict[str, Any]) -> None:
        """Process customer CDC event with SCD Type 2"""
        conn = self._get_warehouse_connection()
        try:
            customer_id = event_data.get('customer_id')
            name = event_data.get('name')
            email = event_data.get('email')
            phone = event_data.get('phone')
            city = event_data.get('city')
            country = event_data.get('country')
            status = event_data.get('status')
            source_ts = event_data.get('updated_at', datetime.utcnow().isoformat())

            with conn.cursor() as cur:
                cur.execute("""
                    CALL sp_load_dim_customers(%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    customer_id, name, email, phone,
                    city, country, status, source_ts
                ))
            conn.commit()
            logger.info(f"Processed customer event: ID={customer_id}")
        except Exception as e:
            logger.error(f"Error processing customer event: {e}", exc_info=True)
            conn.rollback()
            self.error_count += 1
        finally:
            self._put_warehouse_connection(conn)

    def _process_product_event(self, event_data: Dict[str, Any]) -> None:
        """Process product CDC event with SCD Type 2"""
        conn = self._get_warehouse_connection()
        try:
            product_id = event_data.get('product_id')
            name = event_data.get('name')
            category = event_data.get('category')
            price = event_data.get('price')
            status = event_data.get('status')
            source_ts = event_data.get('updated_at', datetime.utcnow().isoformat())

            with conn.cursor() as cur:
                cur.execute("""
                    CALL sp_load_dim_products(%s, %s, %s, %s, %s, %s)
                """, (
                    product_id, name, category, price, status, source_ts
                ))
            conn.commit()
            logger.info(f"Processed product event: ID={product_id}")
        except Exception as e:
            logger.error(f"Error processing product event: {e}", exc_info=True)
            conn.rollback()
            self.error_count += 1
        finally:
            self._put_warehouse_connection(conn)

    def _process_order_event(self, event_data: Dict[str, Any]) -> None:
        """Process order CDC event"""
        conn = self._get_warehouse_connection()
        try:
            order_id = event_data.get('order_id')
            customer_id = event_data.get('customer_id')
            order_date = event_data.get('order_date')
            status = event_data.get('status')
            total_amount = event_data.get('total_amount')
            source_ts = event_data.get('updated_at', datetime.utcnow().isoformat())

            with conn.cursor() as cur:
                cur.execute("""
                    CALL sp_load_facts_orders(%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    order_id, customer_id, order_date, status,
                    0, 0, 0, total_amount, source_ts
                ))
            conn.commit()
            logger.info(f"Processed order event: ID={order_id}, Status={status}")
        except Exception as e:
            logger.error(f"Error processing order event: {e}", exc_info=True)
            conn.rollback()
            self.error_count += 1
        finally:
            self._put_warehouse_connection(conn)

    def _process_order_item_event(self, event_data: Dict[str, Any]) -> None:
        """Process order item CDC event"""
        conn = self._get_warehouse_connection()
        try:
            order_id = event_data.get('order_id')
            order_item_id = event_data.get('order_item_id')
            product_id = event_data.get('product_id')
            quantity = event_data.get('quantity')
            unit_price = event_data.get('unit_price')
            discount = event_data.get('discount', 0)
            source_ts = event_data.get('created_at', datetime.utcnow().isoformat())

            # Get order date from source database
            src_conn = self.source_pool.getconn()
            try:
                with src_conn.cursor() as cur:
                    cur.execute("SELECT order_date FROM orders WHERE order_id = %s", (order_id,))
                    result = cur.fetchone()
                    order_date = result[0] if result else datetime.utcnow()
            finally:
                self.source_pool.putconn(src_conn)

            with conn.cursor() as cur:
                cur.execute("""
                    CALL sp_load_facts_order_items(%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    order_id, order_item_id, product_id, order_date,
                    quantity, unit_price, discount, source_ts
                ))
            conn.commit()
            logger.info(f"Processed order item event: ID={order_item_id}")
        except Exception as e:
            logger.error(f"Error processing order item event: {e}", exc_info=True)
            conn.rollback()
            self.error_count += 1
        finally:
            self._put_warehouse_connection(conn)

    def _process_return_event(self, event_data: Dict[str, Any]) -> None:
        """Process return CDC event"""
        conn = self._get_warehouse_connection()
        try:
            return_id = event_data.get('return_id')
            order_id = event_data.get('order_id')
            product_id = event_data.get('product_id')
            return_date = event_data.get('return_date')
            refund_amount = event_data.get('refund_amount')
            return_reason = event_data.get('return_reason')
            source_ts = event_data.get('created_at', datetime.utcnow().isoformat())

            # Get customer_id from source
            src_conn = self.source_pool.getconn()
            try:
                with src_conn.cursor() as cur:
                    cur.execute("SELECT customer_id FROM orders WHERE order_id = %s", (order_id,))
                    result = cur.fetchone()
                    customer_id = result[0] if result else None
            finally:
                self.source_pool.putconn(src_conn)

            if customer_id:
                with conn.cursor() as cur:
                    cur.execute("""
                        CALL sp_load_facts_returns(%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        return_id, order_id, product_id, customer_id,
                        return_date, refund_amount, return_reason, source_ts
                    ))
                conn.commit()
                logger.info(f"Processed return event: ID={return_id}")
        except Exception as e:
            logger.error(f"Error processing return event: {e}", exc_info=True)
            conn.rollback()
            self.error_count += 1
        finally:
            self._put_warehouse_connection(conn)

    def _process_inventory_event(self, event_data: Dict[str, Any]) -> None:
        """Process inventory movement event (advisory only)"""
        logger.info(
            f"Inventory event: Product={event_data.get('product_id')}, "
            f"Type={event_data.get('movement_type')}, "
            f"Qty={event_data.get('quantity_changed')}"
        )

    def process_event(self, topic: str, event_data: Dict[str, Any]) -> None:
        """Route event to appropriate processor"""
        table_name = self._extract_table_name(topic)
        processor = self.event_processors.get(table_name)

        if processor:
            processor(event_data)
            self.processed_events += 1
        else:
            logger.warning(f"No processor for table: {table_name}")

    def start(self) -> None:
        """Start consuming CDC events"""
        logger.info(f"Starting CDC consumer for topics: {self.kafka_topics}")

        try:
            for message in self.consumer:
                try:
                    if message.value:
                        self.process_event(message.topic, message.value)

                        if self.processed_events % 100 == 0:
                            logger.info(
                                f"Progress: {self.processed_events} events processed, "
                                f"{self.error_count} errors"
                            )
                except Exception as e:
                    logger.error(f"Error processing message: {e}", exc_info=True)
                    self.error_count += 1

        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        finally:
            self.shutdown()

    def shutdown(self) -> None:
        """Cleanup resources"""
        logger.info(
            f"Shutting down. Total processed: {self.processed_events}, Errors: {self.error_count}"
        )
        self.consumer.close()
        self.source_pool.closeall()
        self.warehouse_pool.closeall()


if __name__ == '__main__':
    consumer = CDCStreamConsumer(
        kafka_brokers=['localhost:9092'],
        kafka_topics=['customers', 'products', 'orders', 'order_items', 'returns', 'inventory_movements'],
        source_db_config={
            'host': 'localhost',
            'port': 5432,
            'database': 'transactional_db',
            'user': 'source_user',
            'password': 'source_password'
        },
        warehouse_db_config={
            'host': 'localhost',
            'port': 5433,
            'database': 'analytics_warehouse',
            'user': 'warehouse_user',
            'password': 'warehouse_password'
        }
    )

    consumer.start()
