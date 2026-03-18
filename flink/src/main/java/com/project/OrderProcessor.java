package com.project;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.types.Types;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class OrderProcessor {

        public static void main(String[] args) throws Exception {

                // ─────────────────────────────────────────────
                // 1. Flink Environment
                // ─────────────────────────────────────────────
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.enableCheckpointing(10000);
                env.setParallelism(1);

                ObjectMapper mapper = new ObjectMapper();

                // ─────────────────────────────────────────────
                // 2. Kafka Sources
                // ─────────────────────────────────────────────

                KafkaSource<String> ordersSource = KafkaSource.<String>builder()
                                .setBootstrapServers("kafka:9092")
                                .setTopics("orders")
                                .setGroupId("flink-orders")
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setValueOnlyDeserializer(new SimpleStringSchema())
                                .build();

                KafkaSource<String> orderItemsSource = KafkaSource.<String>builder()
                                .setBootstrapServers("kafka:9092")
                                .setTopics("order_items")
                                .setGroupId("flink-order-items")
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setValueOnlyDeserializer(new SimpleStringSchema())
                                .build();

                KafkaSource<String> customersSource = KafkaSource.<String>builder()
                                .setBootstrapServers("kafka:9092")
                                .setTopics("customers")
                                .setGroupId("flink-customers")
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setValueOnlyDeserializer(new SimpleStringSchema())
                                .build();

                KafkaSource<String> productsSource = KafkaSource.<String>builder()
                                .setBootstrapServers("kafka:9092")
                                .setTopics("products")
                                .setGroupId("flink-products")
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setValueOnlyDeserializer(new SimpleStringSchema())
                                .build();

                DataStream<String> ordersStream = env.fromSource(ordersSource, WatermarkStrategy.noWatermarks(),
                                "Orders");
                DataStream<String> orderItemsStream = env.fromSource(orderItemsSource, WatermarkStrategy.noWatermarks(),
                                "OrderItems");
                DataStream<String> customersStream = env.fromSource(customersSource, WatermarkStrategy.noWatermarks(),
                                "Customers");
                DataStream<String> productsStream = env.fromSource(productsSource, WatermarkStrategy.noWatermarks(),
                                "Products");

                // ─────────────────────────────────────────────
                // 3. Iceberg Catalog
                // ─────────────────────────────────────────────

                Map<String, String> catalogProps = new HashMap<>();
                catalogProps.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.rest.RESTCatalog");
                catalogProps.put(CatalogProperties.URI, "http://iceberg-rest:8181");
                catalogProps.put(CatalogProperties.WAREHOUSE_LOCATION, "/warehouse");

                CatalogLoader catalogLoader = CatalogLoader.custom(
                                "iceberg_catalog",
                                catalogProps,
                                new org.apache.hadoop.conf.Configuration(),
                                "org.apache.iceberg.rest.RESTCatalog");

                Catalog catalog = catalogLoader.loadCatalog();

                // ─────────────────────────────────────────────
                // 4. Create Tables
                // ─────────────────────────────────────────────

                TableIdentifier ordersTable = TableIdentifier.of("db", "orders");
                Schema ordersSchema = new Schema(
                                Types.NestedField.required(1, "order_id", Types.IntegerType.get()),
                                Types.NestedField.required(2, "customer_id", Types.IntegerType.get()),
                                Types.NestedField.required(3, "order_date", Types.StringType.get()));

                if (!catalog.tableExists(ordersTable)) {
                        catalog.createTable(ordersTable, ordersSchema, PartitionSpec.unpartitioned());
                }

                TableIdentifier itemsTable = TableIdentifier.of("db", "order_items");
                Schema itemsSchema = new Schema(
                                Types.NestedField.required(1, "order_item_id", Types.IntegerType.get()),
                                Types.NestedField.required(2, "order_id", Types.IntegerType.get()),
                                Types.NestedField.required(3, "product_id", Types.IntegerType.get()),
                                Types.NestedField.required(4, "quantity", Types.IntegerType.get()),
                                Types.NestedField.required(5, "price", Types.IntegerType.get()));

                if (!catalog.tableExists(itemsTable)) {
                        catalog.createTable(itemsTable, itemsSchema, PartitionSpec.unpartitioned());
                }

                TableIdentifier customersTable = TableIdentifier.of("db", "customers");
                Schema customersSchema = new Schema(
                                Types.NestedField.required(1, "customer_id", Types.IntegerType.get()),
                                Types.NestedField.required(2, "customer_name", Types.StringType.get()),
                                Types.NestedField.required(3, "city", Types.StringType.get()));

                if (!catalog.tableExists(customersTable)) {
                        catalog.createTable(customersTable, customersSchema, PartitionSpec.unpartitioned());
                }

                TableIdentifier productsTable = TableIdentifier.of("db", "products");
                Schema productsSchema = new Schema(
                                Types.NestedField.required(1, "product_id", Types.IntegerType.get()),
                                Types.NestedField.required(2, "product_name", Types.StringType.get()),
                                Types.NestedField.required(3, "category", Types.StringType.get()),
                                Types.NestedField.required(4, "price", Types.IntegerType.get()));

                if (!catalog.tableExists(productsTable)) {
                        catalog.createTable(productsTable, productsSchema, PartitionSpec.unpartitioned());
                }

                // ─────────────────────────────────────────────
                // 5. Transform Streams
                // ─────────────────────────────────────────────

                DataStream<RowData> ordersRow = ordersStream.map(value -> {
                        try {
                                JsonNode json = mapper.readTree(value);
                                GenericRowData row = new GenericRowData(3);
                                row.setField(0, json.get("order_id").asInt());
                                row.setField(1, json.get("customer_id").asInt());
                                row.setField(2, StringData.fromString(json.get("order_date").asText()));
                                return row;
                        } catch (Exception e) {
                                System.out.println("Bad Orders JSON: " + value);
                                return null;
                        }
                }, InternalTypeInfo.of(RowType.of(new IntType(), new IntType(), new VarCharType())))
                                .filter(Objects::nonNull);

                DataStream<RowData> itemsRow = orderItemsStream.map(value -> {
                        try {
                                JsonNode json = mapper.readTree(value);
                                GenericRowData row = new GenericRowData(5);
                                row.setField(0, json.get("order_item_id").asInt());
                                row.setField(1, json.get("order_id").asInt());
                                row.setField(2, json.get("product_id").asInt());
                                row.setField(3, json.get("quantity").asInt());
                                row.setField(4, json.get("price").asInt());
                                return row;
                        } catch (Exception e) {
                                System.out.println("Bad Items JSON: " + value);
                                return null;
                        }
                }, InternalTypeInfo.of(
                                RowType.of(new IntType(), new IntType(), new IntType(), new IntType(), new IntType())))
                                .filter(Objects::nonNull);

                DataStream<RowData> customersRow = customersStream.map(value -> {
                        try {
                                JsonNode json = mapper.readTree(value);
                                GenericRowData row = new GenericRowData(3);
                                row.setField(0, json.get("customer_id").asInt());
                                row.setField(1, StringData.fromString(json.get("customer_name").asText()));
                                row.setField(2, StringData.fromString(json.get("city").asText()));
                                return row;
                        } catch (Exception e) {
                                System.out.println("Bad Customers JSON: " + value);
                                return null;
                        }
                }, InternalTypeInfo.of(RowType.of(new IntType(), new VarCharType(), new VarCharType())))
                                .filter(Objects::nonNull);

                DataStream<RowData> productsRow = productsStream.map(value -> {
                        try {
                                JsonNode json = mapper.readTree(value);
                                GenericRowData row = new GenericRowData(4);
                                row.setField(0, json.get("product_id").asInt());
                                row.setField(1, StringData.fromString(json.get("product_name").asText()));
                                row.setField(2, StringData.fromString(json.get("category").asText()));
                                row.setField(3, json.get("price").asInt());
                                return row;
                        } catch (Exception e) {
                                System.out.println("Bad Products JSON: " + value);
                                return null;
                        }
                }, InternalTypeInfo.of(RowType.of(new IntType(), new VarCharType(), new VarCharType(), new IntType())))
                                .filter(Objects::nonNull);

                // ─────────────────────────────────────────────
                // 6. Iceberg Sink
                // ─────────────────────────────────────────────

                FlinkSink.forRowData(ordersRow)
                                .tableLoader(TableLoader.fromCatalog(catalogLoader, ordersTable))
                                .append();

                FlinkSink.forRowData(itemsRow)
                                .tableLoader(TableLoader.fromCatalog(catalogLoader, itemsTable))
                                .append();

                FlinkSink.forRowData(customersRow)
                                .tableLoader(TableLoader.fromCatalog(catalogLoader, customersTable))
                                .writeParallelism(1)
                                .append();

                FlinkSink.forRowData(productsRow)
                                .tableLoader(TableLoader.fromCatalog(catalogLoader, productsTable))
                                .writeParallelism(1)
                                .append();

                env.execute("Kafka → Flink → Iceberg (OLTP Pipeline)");
        }
}
