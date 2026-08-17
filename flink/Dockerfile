FROM flink:1.20.1

# Kafka connector for Flink
RUN wget -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar

# PyFlink
RUN pip install apache-flink==1.20.1