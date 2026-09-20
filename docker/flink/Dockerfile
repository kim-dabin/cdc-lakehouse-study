FROM flink:1.20.4-scala_2.12-java17
USER root
RUN set -eux; \
 wget -q -P /opt/flink/lib https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar; \
 wget -q -P /opt/flink/lib https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/1.10.0/iceberg-flink-runtime-1.20-1.10.0.jar; \
 wget -q -P /opt/flink/lib https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.10.0/iceberg-aws-bundle-1.10.0.jar; \
 wget -q -P /opt/flink/lib https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.7/postgresql-42.7.7.jar; \
 wget -q -P /opt/flink/lib https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar; \
 mkdir -p /opt/flink/state/checkpoints /opt/flink/state/savepoints; chown -R flink:flink /opt/flink/state
USER flink
