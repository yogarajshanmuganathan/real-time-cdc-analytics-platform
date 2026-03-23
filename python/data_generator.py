"""
Data generation utilities for CDC testing
Generates realistic transaction data to feed CDC pipeline
"""

import random
import psycopg2
from datetime import datetime, timedelta
from typing import Dict, List
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TransactionalDataGenerator:
    """Generate realistic e-commerce transactional data"""

    def __init__(self, connection_config: Dict):
        self.conn = psycopg2.connect(**connection_config)
        self.cursor = self.conn.cursor()

    def generate_initial_data(self, customers: int = 50, products: int = 100, orders: int = 200):
        """Generate initial dataset"""
        logger.info(f"Generating {customers} customers, {products} products, {orders} orders...")

        # Generate customers
        self.cursor.execute("CALL generate_sample_customers(%s)", (customers,))
        self.conn.commit()
        logger.info(f"✓ Generated {customers} customers")

        # Generate products
        self.cursor.execute("CALL generate_sample_products(%s)", (products,))
        self.conn.commit()
        logger.info(f"✓ Generated {products} products")

        # Generate orders
        self.cursor.execute("CALL generate_sample_orders(%s)", (orders,))
        self.conn.commit()
        logger.info(f"✓ Generated {orders} orders")

    def simulate_real_time_orders(self, orders_per_minute: int = 10, duration_minutes: int = 60):
        """Simulate real-time order transactions"""
        logger.info(f"Starting real-time simulation: {orders_per_minute} orders/min for {duration_minutes} minutes")

        start_time = datetime.now()
        end_time = start_time + timedelta(minutes=duration_minutes)
        events_generated = 0

        while datetime.now() < end_time:
            # Generate orders
            for _ in range(orders_per_minute):
                try:
                    self.cursor.execute("CALL generate_sample_orders(1)")
                    self.conn.commit()
                    events_generated += 1

                    # Randomly generate returns (5% of orders)
                    if random.random() < 0.05:
                        self._generate_return()

                    # Randomly update customer info (product changes)
                    if random.random() < 0.02:
                        self._update_customer_info()

                except Exception as e:
                    logger.error(f"Error generating order: {e}")
                    self.conn.rollback()

            logger.info(f"Generated {events_generated} events so far...")
            
            # Wait for next batch
            elapsed = (datetime.now() - start_time).total_seconds()
            expected_time = (events_generated / orders_per_minute) * 60
            sleep_time = max(0, expected_time - elapsed)
            
            if sleep_time > 0:
                import time
                time.sleep(min(sleep_time, 5))  # Max 5 seconds sleep

        logger.info(f"Simulation complete. Generated {events_generated} events")

    def _generate_return(self):
        """Generate a random return"""
        self.cursor.execute("""
            SELECT order_id, product_id FROM order_items 
            WHERE order_id NOT IN (SELECT order_id FROM returns)
            ORDER BY RANDOM() LIMIT 1
        """)
        result = self.cursor.fetchone()
        if result:
            order_id, product_id = result
            self.cursor.execute(
                "CALL process_return(%s, %s, %s)",
                (order_id, product_id, "Customer requested return")
            )
            self.conn.commit()
            logger.info(f"Generated return for order {order_id}")

    def _update_customer_info(self):
        """Randomly update customer information (triggers SCD Type 2)"""
        self.cursor.execute("SELECT customer_id FROM customers ORDER BY RANDOM() LIMIT 1")
        result = self.cursor.fetchone()
        if result:
            customer_id = result[0]
            new_city = random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'])
            self.cursor.execute(
                "UPDATE customers SET city = %s WHERE customer_id = %s",
                (new_city, customer_id)
            )
            self.conn.commit()
            logger.info(f"Updated customer {customer_id} (City: {new_city})")

    def get_order_count(self) -> int:
        """Get current order count"""
        self.cursor.execute("SELECT COUNT(*) FROM orders")
        return self.cursor.fetchone()[0]

    def close(self):
        """Close database connection"""
        self.cursor.close()
        self.conn.close()


if __name__ == '__main__':
    gen = TransactionalDataGenerator({
        'host': 'localhost',
        'port': 5432,
        'database': 'transactional_db',
        'user': 'source_user',
        'password': 'source_password'
    })

    try:
        # Generate initial data
        gen.generate_initial_data(customers=100, products=150, orders=300)

        # Simulate real-time activity
        gen.simulate_real_time_orders(orders_per_minute=20, duration_minutes=5)

    finally:
        gen.close()
        logger.info("Data generation complete")
