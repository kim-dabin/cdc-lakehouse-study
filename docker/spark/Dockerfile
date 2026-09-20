FROM apache/spark:3.5.6-java17
USER root
RUN set -eux; \
 curl -fsSL https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.0/iceberg-spark-runtime-3.5_2.12-1.10.0.jar -o /opt/spark/jars/iceberg.jar; \
 curl -fsSL https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.10.0/iceberg-aws-bundle-1.10.0.jar -o /opt/spark/jars/iceberg-aws.jar; \
 curl -fsSL https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.7/postgresql-42.7.7.jar -o /opt/spark/jars/postgresql.jar
USER spark
COPY spark/start-thrift.sh /opt/start-thrift.sh
ENTRYPOINT ["bash", "/opt/start-thrift.sh"]
