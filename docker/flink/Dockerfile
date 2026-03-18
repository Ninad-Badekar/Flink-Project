FROM flink:1.18

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Iceberg runtime
RUN curl -L https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.18/1.5.2/iceberg-flink-runtime-1.18-1.5.2.jar \
    -o /opt/flink/lib/iceberg-flink-runtime-1.18-1.5.2.jar

# Hadoop dependencies
RUN curl -L https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar \
    -o /opt/flink/lib/hadoop-client-api-3.3.6.jar

RUN curl -L https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar \
    -o /opt/flink/lib/hadoop-client-runtime-3.3.6.jar

# Flink Table Runtime
RUN curl -L https://repo.maven.apache.org/maven2/org/apache/flink/flink-table-runtime/1.18.0/flink-table-runtime-1.18.0.jar \
    -o /opt/flink/lib/flink-table-runtime-1.18.0.jar

RUN curl -L https://repo.maven.apache.org/maven2/org/apache/flink/flink-table-common/1.18.0/flink-table-common-1.18.0.jar \
    -o /opt/flink/lib/flink-table-common-1.18.0.jar



# Logging
RUN curl -L https://repo.maven.apache.org/maven2/commons-logging/commons-logging/1.2/commons-logging-1.2.jar \
    -o /opt/flink/lib/commons-logging-1.2.jar

# Copy job JAR
COPY flink-kafka-job-1.0.jar /opt/flink/usrlib/flink-kafka-job-1.0.jar
