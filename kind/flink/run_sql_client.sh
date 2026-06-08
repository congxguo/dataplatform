kubectl apply -f flink-session.yaml

k exec -it flink-session-xxx -n flink -- bash

./bin/sql-client.sh

CREATE CATALOG iceberg_catalog WITH (
  'type'='iceberg',
  'catalog-type'='rest',
  'uri'='http://iceberg-rest-catalog.iceberg.svc.cluster.local:8181',
  'warehouse'='s3://iceberg-warehouse',
  'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint'='http://minio.minio.svc.cluster.local:9000',
  's3.path-style-access'='true',
  's3.access-key'='minioadmin',
  's3.secret-key'='minioadmin',
  'client.region'='us-east-1'
);

USE CATALOG iceberg_catalog;

SHOW DATABASES;

USE demo_db;

SHOW TABLES;

DESCRIBE users;

DESCRIBE EXTENDED users;

SELECT * FROM users;

SELECT * FROM "users$snapshots";

SELECT * FROM "users$history";

SELECT * FROM "users$files";

SELECT * FROM users WHERE age > 30;

SELECT COUNT(*) FROM users;
