"""
Analytics Queries - Real-time Business Intelligence
Execute against warehouse for real-time insights
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import json
from datetime import datetime, timedelta


class WarehouseAnalytics:
    """Execute analytical queries against warehouse"""

    def __init__(self, connection_config):
        self.conn = psycopg2.connect(**connection_config)

    def get_daily_sales_summary(self, start_date=None, end_date=None):
        """Daily sales KPIs"""
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).date()
        if not end_date:
            end_date = datetime.now().date()

        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT 
                    date,
                    year,
                    month,
                    total_orders,
                    unique_customers,
                    total_revenue,
                    avg_order_value,
                    total_items,
                    total_discounts
                FROM vw_daily_sales_summary
                WHERE date BETWEEN %s AND %s
                ORDER BY date DESC
            """, (start_date, end_date))
            return cur.fetchall()

    def get_customer_lifetime_value_top_100(self):
        """Top 100 customers by lifetime value"""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT 
                    customer_sk,
                    customer_id,
                    name,
                    total_orders,
                    lifetime_revenue,
                    avg_order_value,
                    total_items_purchased,
                    customer_segment
                FROM vw_customer_lifetime_value
                WHERE lifetime_revenue > 0
                ORDER BY lifetime_revenue DESC
                LIMIT 100
            """)
            return cur.fetchall()

    def get_product_performance(self, limit=50):
        """Top products by revenue"""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT 
                    product_sk,
                    product_id,
                    name,
                    category,
                    times_ordered,
                    total_units_sold,
                    total_revenue,
                    avg_selling_price
                FROM vw_product_performance
                WHERE total_revenue > 0
                ORDER BY total_revenue DESC
                LIMIT %s
            """, (limit,))
            return cur.fetchall()

    def get_category_performance(self):
        """Sales by product category"""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT 
                    category,
                    COUNT(DISTINCT product_sk) as product_count,
                    SUM(total_units_sold) as total_units,
                    SUM(total_revenue)::DECIMAL(12, 2) as category_revenue,
                    AVG(total_revenue)::DECIMAL(12, 2) as avg_product_revenue
                FROM vw_product_performance
                GROUP BY category
                ORDER BY category_revenue DESC
            """)
            return cur.fetchall()

    def get_return_analysis(self):
        """Return rate and reasons"""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT 
                    product_name,
                    category,
                    total_returns,
                    total_refunded,
                    return_reason,
                    return_percentage
                FROM vw_return_analysis
                WHERE total_returns > 0
                ORDER BY total_returns DESC
            """)
            return cur.fetchall()

    def get_order_status_distribution(self):
        """Orders by status"""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT 
                    dos.status_description,
                    dos.status_category,
                    COUNT(*) as order_count,
                    SUM(fo.final_amount)::DECIMAL(12, 2) as status_revenue,
                    AVG(fo.final_amount)::DECIMAL(12, 2) as avg_amount
                FROM facts_orders fo
                JOIN dim_order_status dos ON fo.status_sk = dos.status_sk
                GROUP BY dos.status_description, dos.status_category
                ORDER BY order_count DESC
            """)
            return cur.fetchall()

    def get_monthly_trend(self):
        """Month-over-month trend"""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT 
                    year,
                    month,
                    MAKE_DATE(year, month, 1) as period_start,
                    COUNT(DISTINCT total_orders) as total_orders,
                    SUM(total_revenue)::DECIMAL(12, 2) as monthly_revenue,
                    ROUND(SUM(total_revenue) / NULLIF(SUM(total_orders), 0), 2)::DECIMAL(12,2) as aov
                FROM vw_daily_sales_summary
                GROUP BY year, month
                ORDER BY year DESC, month DESC
            """)
            return cur.fetchall()

    def get_customer_acquisition_trend(self, days=90):
        """New customers by day"""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT 
                    dd.date,
                    COUNT(DISTINCT dc.customer_sk) as new_customers
                FROM dim_customers dc
                JOIN dim_date dd ON dd.date_sk = CAST(TO_CHAR(dc.effective_date, 'YYYYMMDD') AS INT)
                WHERE dc.effective_date >= CURRENT_DATE - INTERVAL '%s days'
                  AND dc.effective_date = dc.effective_date  -- Only original records
                GROUP BY dd.date
                ORDER BY dd.date DESC
            """, (days,))
            return cur.fetchall()

    def close(self):
        """Close connection"""
        self.conn.close()


def print_report(title, data, max_rows=10):
    """Pretty print report"""
    print(f"\n{'='*80}")
    print(f"{title.upper()}")
    print(f"{'='*80}\n")

    if isinstance(data, list) and len(data) > 0:
        first_row = data[0]
        headers = list(first_row.keys())

        # Print headers
        row_str = " | ".join(f"{h:20}" for h in headers)
        print(row_str)
        print("-" * len(row_str))

        # Print rows
        for i, row in enumerate(data[:max_rows]):
            row_str = " | ".join(
                f"{str(row[h])[:20]:20}" for h in headers
            )
            print(row_str)

        if len(data) > max_rows:
            print(f"\n... and {len(data) - max_rows} more rows")
    else:
        print("No data")


if __name__ == '__main__':
    analytics = WarehouseAnalytics({
        'host': 'localhost',
        'port': 5433,
        'database': 'analytics_warehouse',
        'user': 'warehouse_user',
        'password': 'warehouse_password'
    })

    try:
        print_report("Daily Sales Summary (Last 30 Days)", analytics.get_daily_sales_summary())
        print_report("Top 10 Customers by LTV", analytics.get_customer_lifetime_value_top_100(), max_rows=10)
        print_report("Top 20 Products by Revenue", analytics.get_product_performance(limit=20), max_rows=20)
        print_report("Category Performance", analytics.get_category_performance())
        print_report("Order Status Distribution", analytics.get_order_status_distribution())
        print_report("Monthly Trends", analytics.get_monthly_trend())
        print_report("Return Analysis", analytics.get_return_analysis())

    finally:
        analytics.close()
