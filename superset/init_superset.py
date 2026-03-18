#!/usr/bin/env python3
"""
Auto-init script for Superset.
Run this inside the superset container to set up Trino connection and saved queries.
"""

import os

os.environ["SUPERSET_CONFIG_PATH"] = "/app/pythonpath/superset_config.py"

from superset.app import create_app

app = create_app()

QUERIES = [
    {
        "label": "Total Revenue",
        "sql": """SELECT SUM(quantity * price) AS total_revenue
FROM iceberg.db.order_items""",
    },
    {
        "label": "Revenue by Customer",
        "sql": """SELECT
    c.customer_name,
    c.city,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(oi.quantity * oi.price) AS total_revenue
FROM iceberg.db.orders o
JOIN iceberg.db.customers c ON o.customer_id = c.customer_id
JOIN iceberg.db.order_items oi ON o.order_id = oi.order_id
GROUP BY c.customer_name, c.city
ORDER BY total_revenue DESC""",
    },
    {
        "label": "Top Products",
        "sql": """SELECT
    p.product_name,
    p.category,
    SUM(oi.quantity) AS units_sold,
    SUM(oi.quantity * oi.price) AS revenue
FROM iceberg.db.order_items oi
JOIN iceberg.db.products p ON oi.product_id = p.product_id
GROUP BY p.product_name, p.category
ORDER BY units_sold DESC""",
    },
    {
        "label": "Orders Over Time",
        "sql": """SELECT
    DATE_TRUNC('minute', from_iso8601_timestamp(order_date)) AS order_minute,
    COUNT(order_id) AS order_count
FROM iceberg.db.orders
GROUP BY DATE_TRUNC('minute', from_iso8601_timestamp(order_date))
ORDER BY order_minute""",
    },
    {
        "label": "Revenue by Product Category",
        "sql": """SELECT
    p.category,
    SUM(oi.quantity * oi.price) AS revenue,
    SUM(oi.quantity) AS units_sold
FROM iceberg.db.order_items oi
JOIN iceberg.db.products p ON oi.product_id = p.product_id
GROUP BY p.category
ORDER BY revenue DESC""",
    },
    {
        "label": "Customer Order Summary",
        "sql": """SELECT
    c.customer_name,
    c.city,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(oi.quantity) AS total_items,
    SUM(oi.quantity * oi.price) AS total_spent
FROM iceberg.db.customers c
LEFT JOIN iceberg.db.orders o ON c.customer_id = o.customer_id
LEFT JOIN iceberg.db.order_items oi ON o.order_id = oi.order_id
GROUP BY c.customer_name, c.city
ORDER BY total_spent DESC""",
    },
]


def init():
    with app.app_context():
        from flask_appbuilder.security.sqla.models import User

        from superset import db
        from superset.models.core import Database
        from superset.models.sql_lab import SavedQuery

        # 1. Create Trino database connection
        existing = db.session.query(Database).filter_by(database_name="Trino").first()
        if not existing:
            trino_db = Database(
                database_name="Trino",
                sqlalchemy_uri="trino://admin@trino:8080/iceberg",
                expose_in_sqllab=True,
                allow_run_async=False,
                allow_ctas=False,
                allow_cvas=False,
                allow_dml=False,
            )
            db.session.add(trino_db)
            db.session.commit()
            print("✅ Trino database connection created")
        else:
            trino_db = existing
            print("ℹ️  Trino connection already exists")

        # 2. Get admin user
        admin = db.session.query(User).filter_by(username="admin").first()
        if not admin:
            print("⚠️  Admin user not found - run superset fab create-admin first")
            return

        # 3. Create saved queries
        for q in QUERIES:
            existing_q = (
                db.session.query(SavedQuery)
                .filter_by(label=q["label"], created_by=admin)
                .first()
            )
            if not existing_q:
                saved = SavedQuery(
                    label=q["label"],
                    sql=q["sql"],
                    db_id=trino_db.id,
                    schema="db",
                    created_by=admin,
                )
                db.session.add(saved)
                print(f"✅ Saved query: {q['label']}")
            else:
                print(f"ℹ️  Query already exists: {q['label']}")

        db.session.commit()
        print("\n✅ Superset init complete! Go to SQL → Saved Queries to see them.")


if __name__ == "__main__":
    init()
