import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers="localhost:29092",  # host → correct
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    linger_ms=0,
)


#  Static master data

customers = [
    {"customer_id": 1, "customer_name": "Ninad", "city": "Mumbai"},
    {"customer_id": 2, "customer_name": "Rahul", "city": "Pune"},
    {"customer_id": 3, "customer_name": "Amit", "city": "Delhi"},
]

products = [
    {
        "product_id": 101,
        "product_name": "Laptop",
        "category": "Electronics",
        "price": 50000,
    },
    {
        "product_id": 102,
        "product_name": "Phone",
        "category": "Electronics",
        "price": 20000,
    },
    {
        "product_id": 103,
        "product_name": "Keyboard",
        "category": "Accessories",
        "price": 2000,
    },
    {
        "product_id": 104,
        "product_name": "Mouse",
        "category": "Accessories",
        "price": 1000,
    },
]

#  Send master data ONCE

print("🔄 Sending Customers & Products (ONE-TIME)...")

for customer in customers:
    producer.send("customers", customer)

for product in products:
    producer.send("products", product)

producer.flush()
print("✅ Master data sent\n")

# Stream transactional data

order_id = 1
order_item_id = 1

while True:

    customer = random.choice(customers)
    product = random.choice(products)

    order = {
        "order_id": order_id,
        "customer_id": customer["customer_id"],
        "order_date": datetime.now().isoformat(),
    }

    quantity = random.randint(1, 3)

    order_item = {
        "order_item_id": order_item_id,
        "order_id": order_id,
        "product_id": product["product_id"],
        "quantity": quantity,
        "price": product["price"],
    }
    producer.send("orders", order)
    producer.send("order_items", order_item)

    producer.flush()

    print("📦 Order:", order)
    print("🧾 Order Item:", order_item)

    order_id += 1
    order_item_id += 1

    time.sleep(1)
