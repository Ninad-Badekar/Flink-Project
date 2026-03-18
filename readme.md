# Real-Time OLTP Data Pipeline
### Kafka → Flink → Iceberg → Trino → Superset

A real-time streaming data pipeline that ingests order events from Kafka, processes them with Apache Flink, stores them in Apache Iceberg tables, and exposes them for analytics via Trino and Apache Superset.

---

## Architecture

```
Kafka Producer
     │
     ▼
Kafka Topics (orders, order_items, customers, products)
     │
     ▼
Apache Flink 1.18  ──►  Iceberg REST Catalog
     │                        │
     ▼                        ▼
Apache Iceberg 1.5.2 (Parquet files on /warehouse)
     │
     ▼
Trino (SQL query engine)
     │
     ▼
Apache Superset (Dashboards & SQL Lab)
```

| Layer         | Technology              | Purpose                              |
|---------------|-------------------------|--------------------------------------|
| Ingestion     | Apache Kafka            | Message broker for streaming events  |
| Processing    | Apache Flink 1.18       | Stream processing & transformation   |
| Storage       | Apache Iceberg 1.5.2    | Open table format with ACID support  |
| Catalog       | Iceberg REST Catalog    | Table metadata management            |
| Query         | Trino                   | Distributed SQL query engine         |
| Visualization | Apache Superset         | BI dashboards and SQL Lab            |

---

## Prerequisites

- Docker & Docker Compose
- Java 11+ (to build the Flink job)
- Maven 3.6+ (to build the Flink job)
- Python 3.8+ with pip (for the Kafka producer)
- At least 8GB RAM for Docker
- Ports available: `8080` (Trino), `8081` (Flink), `8088` (Superset), `8181` (Iceberg REST), `9092` (Kafka)

---

## Project Structure

```
Flink-Project/
├── docker/
│   ├── docker-compose.yml          # All services
│   ├── flink/
│   │   ├── Dockerfile              # Flink image with Iceberg jars
│   │   └── flink-kafka-job-1.0.jar
│   ├── superset/
│   │   ├── Dockerfile              # Superset with Trino driver
│   │   └── entrypoint.sh           # Auto-init on container start
│   └── trino/
│       └── iceberg.properties      # Trino → Iceberg catalog config
├── flink/
│   ├── pom.xml
│   └── src/main/java/com/project/OrderProcessor.java
├── producer/
│   └── orders_producer.py          # Kafka data producer
├── superset/
│   ├── superset_config.py          # Superset Flask config
│   └── init_superset.py            # Auto-creates Trino connection + saved queries
└── warehouse/                      # Iceberg data files (auto-created)
    └── db/
        ├── customers/
        ├── products/
        ├── orders/
        └── order_items/
```

---

## Setup & Installation

### Step 1 — Create the warehouse directory

```bash
cd ~/path/to/Flink-Project
mkdir -p warehouse
chmod -R 777 warehouse
```

### Step 2 — Build the Flink job (if needed)

```bash
cd flink/
mvn clean package -DskipTests
cp target/flink-kafka-job-1.0.jar ../docker/flink/
```

### Step 3 — Start all services

```bash
cd docker/
docker-compose up -d --build
```

Wait ~60 seconds for all services to initialize, then verify:

```bash
docker ps
```

You should see these containers running:
- `docker-zookeeper-1`
- `docker-kafka-1`
- `docker-flink-jobmanager-1`
- `docker-flink-taskmanager-1`
- `docker-iceberg-rest-1`
- `docker-trino-1`
- `superset`

### Step 4 — Submit the Flink job

```bash
docker exec -it docker-flink-jobmanager-1 flink run \
  -c com.project.OrderProcessor \
  /opt/flink/usrlib/flink-kafka-job-1.0.jar
```

Verify the job is running at **http://localhost:8081**

### Step 5 — Start the Kafka producer

```bash
cd producer/
pip install kafka-python
python orders_producer.py
```

The producer continuously streams orders, order items, customers, and products into Kafka.

---

## Service URLs & Credentials

| Service             | URL                        | Username | Password |
|---------------------|----------------------------|----------|----------|
| Flink Dashboard     | http://localhost:8081      | —        | —        |
| Trino UI            | http://localhost:8080      | admin    | —        |
| Iceberg REST Catalog| http://localhost:8181      | —        | —        |
| Apache Superset     | http://localhost:8088      | admin    | admin    |
| Kafka (external)    | localhost:29092             | —        | —        |

---

## Kafka Topics & Schemas

| Topic        | Key Fields                                              |
|--------------|---------------------------------------------------------|
| `customers`  | `customer_id`, `customer_name`, `city`                 |
| `products`   | `product_id`, `product_name`, `category`, `price`      |
| `orders`     | `order_id`, `customer_id`, `order_date`                |
| `order_items`| `order_item_id`, `order_id`, `product_id`, `quantity`, `price` |

### Example messages

```json
// customers
{"customer_id": 1, "customer_name": "Alice", "city": "Mumbai"}

// products
{"product_id": 101, "product_name": "Laptop", "category": "Electronics", "price": 50000}

// orders
{"order_id": 1, "customer_id": 1, "order_date": "2026-03-18T10:00:00.000000"}

// order_items
{"order_item_id": 1, "order_id": 1, "product_id": 101, "quantity": 2, "price": 50000}
```

---

## Iceberg Tables

Tables are auto-created by the Flink job on first run in the `db` namespace.

| Table            | Schema                                                                 |
|------------------|------------------------------------------------------------------------|
| `db.customers`   | `customer_id` INT, `customer_name` VARCHAR, `city` VARCHAR            |
| `db.products`    | `product_id` INT, `product_name` VARCHAR, `category` VARCHAR, `price` INT |
| `db.orders`      | `order_id` INT, `customer_id` INT, `order_date` VARCHAR               |
| `db.order_items` | `order_item_id` INT, `order_id` INT, `product_id` INT, `quantity` INT, `price` INT |

---

## Querying with Trino

Connect to the Trino CLI:

```bash
docker exec -it docker-trino-1 trino
```

### Verify tables

```sql
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.db;
```

### Revenue by customer

```sql
SELECT
    c.customer_name,
    c.city,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(oi.quantity * oi.price) AS total_revenue
FROM iceberg.db.orders o
JOIN iceberg.db.customers c ON o.customer_id = c.customer_id
JOIN iceberg.db.order_items oi ON o.order_id = oi.order_id
GROUP BY c.customer_name, c.city
ORDER BY total_revenue DESC;
```

### Top products

```sql
SELECT
    p.product_name,
    p.category,
    SUM(oi.quantity) AS units_sold,
    SUM(oi.quantity * oi.price) AS revenue
FROM iceberg.db.order_items oi
JOIN iceberg.db.products p ON oi.product_id = p.product_id
GROUP BY p.product_name, p.category
ORDER BY units_sold DESC;
```

### Orders over time

```sql
SELECT
    DATE_TRUNC('minute', from_iso8601_timestamp(order_date)) AS order_minute,
    COUNT(order_id) AS order_count
FROM iceberg.db.orders
GROUP BY DATE_TRUNC('minute', from_iso8601_timestamp(order_date))
ORDER BY order_minute;
```

---

## Apache Superset

Open **http://localhost:8088** and log in with `admin` / `admin`.

The following are automatically created on startup:

- **Database connection:** Trino (`trino://admin@trino:8080/iceberg`)
- **Saved Queries** (SQL Lab → Saved Queries):
  - Total Revenue
  - Revenue by Customer
  - Top Products
  - Orders Over Time
  - Revenue by Product Category
  - Customer Order Summary

### Creating charts

1. Go to **SQL → Saved Queries**
2. Open a query and click **Run**
3. Click **Create Chart** from the results
4. Select chart type and configure axes/metrics
5. Save the chart

### Building a dashboard

1. Go to **Dashboards → + Dashboard**
2. Name it `OLTP Sales Dashboard`
3. Click **Edit Dashboard**
4. Drag and drop saved charts from the right panel
5. Click **Save**

---

## Configuration Reference

### `trino/iceberg.properties`

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://iceberg-rest:8181
iceberg.rest-catalog.warehouse=/warehouse
fs.hadoop.enabled=true
```

### Flink job settings

| Setting               | Value                      | Notes                                  |
|-----------------------|----------------------------|----------------------------------------|
| Checkpointing interval| 10,000 ms                  | Data committed to Iceberg every 10s    |
| Parallelism           | 1                          | Single-threaded for development        |
| Kafka bootstrap       | `kafka:9092`               | Internal Docker network address        |
| Iceberg catalog URI   | `http://iceberg-rest:8181` | REST catalog endpoint                  |
| Warehouse path        | `/warehouse`               | Mounted volume from host               |

---

## Troubleshooting

### Flink job not writing data

```bash
# Check if /warehouse is writable inside the container
docker exec -it docker-flink-taskmanager-1 touch /warehouse/test.txt && echo "WRITABLE"

# Fix permissions and recreate containers
sudo chmod -R 777 ../warehouse
docker-compose up -d --force-recreate flink-jobmanager flink-taskmanager
```

Check checkpoint status at **http://localhost:8081** → Jobs → Checkpoints tab. All checkpoints should show `Completed`.

### Trino shows empty tables

```bash
# Check if Iceberg REST catalog has the tables registered
curl http://localhost:8181/v1/namespaces/db/tables

# Check if Parquet files exist on disk
ls ../warehouse/db/
```

### Superset "Failed to start remote query on a worker"

Celery is not configured. Ensure `allow_run_async=False` is set in `init_superset.py` when creating the Trino database connection.

### Full stack restart

```bash
cd docker/
docker-compose down
sudo chmod -R 777 ../warehouse
docker-compose up -d --build
```

### Clean restart (wipes all data)

```bash
docker-compose down -v
rm -rf ../warehouse/db
mkdir -p ../warehouse
chmod -R 777 ../warehouse
docker-compose up -d --build
```

---

## Tech Versions

| Component      | Version  |
|----------------|----------|
| Apache Flink   | 1.18.1   |
| Apache Iceberg | 1.5.2    |
| Kafka          | 3.5.0    |
| Trino          | 479      |
| Superset       | Latest   |
| Java           | 11       |
