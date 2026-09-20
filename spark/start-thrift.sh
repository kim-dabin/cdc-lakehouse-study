#!/usr/bin/env bash
set -euo pipefail
exec /opt/spark/bin/spark-submit \
 --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 \
 --master 'local[2]' --driver-memory 1536m \
 --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
 --conf spark.sql.defaultCatalog=lake \
 --conf spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog \
 --conf spark.sql.catalog.lake.cache-enabled=false \
 --conf spark.sql.catalog.lake.type=jdbc \
 --conf spark.sql.catalog.lake.uri=jdbc:postgresql://catalog-db:5432/catalog \
 --conf spark.sql.catalog.lake.jdbc.user=catalog \
 --conf "spark.sql.catalog.lake.jdbc.password=${CATALOG_PASSWORD}" \
 --conf spark.sql.catalog.lake.warehouse=s3://warehouse/ \
 --conf spark.sql.catalog.lake.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
 --conf spark.sql.catalog.lake.s3.endpoint=http://minio:9000 \
 --conf spark.sql.catalog.lake.s3.path-style-access=true \
 --conf spark.sql.catalog.lake.client.region=us-east-1 \
 --conf spark.sql.shuffle.partitions=2 \
 --conf spark.sql.session.timeZone=UTC \
 --conf spark.sql.hive.thriftServer.singleSession=true \
 --hiveconf hive.server2.thrift.port=10000 \
 --hiveconf hive.server2.thrift.bind.host=0.0.0.0 \
 --hiveconf hive.server2.authentication=NOSASL
