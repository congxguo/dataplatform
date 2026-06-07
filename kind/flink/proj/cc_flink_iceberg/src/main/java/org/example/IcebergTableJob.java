package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class IcebergTableJob {

    public static void main(String[] args) throws Exception {
       final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
       StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

       // 1. Configure Iceberg Catalog
       tableEnv.executeSql(
             "CREATE CATALOG iceberg_catalog WITH (" +
                   "  'type'='iceberg'," +
                   "  'catalog-type'='rest'," +
                   "  'uri'='http://iceberg-rest-catalog.iceberg.svc.cluster.local:8181'," +
                   "  'warehouse'='s3://iceberg-warehouse'," +
                   "  'io-impl'='org.apache.iceberg.aws.s3.S3FileIO'," +
                   "  's3.endpoint'='http://minio.minio.svc.cluster.local:9000'," +
                   "  's3.path-style-access'='true'," +
                   "  's3.access-key'='minioadmin'," +
                   "  's3.secret-key'='minioadmin'," +
                   "  'client.region'='us-east-1'" +
                   ")"
       );

       tableEnv.executeSql("USE CATALOG iceberg_catalog");
       tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS demo_db");
       tableEnv.executeSql("USE demo_db");

       // 2. DDL - These do not trigger a Flink Job
       tableEnv.executeSql("DROP TABLE IF EXISTS users");
       tableEnv.executeSql(
             "CREATE TABLE users (" +
                   "  id BIGINT," +
                   "  name STRING," +
                   "  age INT," +
                   "  city STRING," +
                   "  PRIMARY KEY (id) NOT ENFORCED" +
                   ") WITH (" +
                   "  'format-version'='2'," +
                   "  'write.upsert.enabled'='true'" +
                   ")"
       );

       // 3. Execution - Trigger EXACTLY ONE job
       // Option A: If you just want to load data, use executeSql(...).await()
       // Option B: If you want to verify data immediately, use a TableResult and collect()

       System.out.println("Submitting Insert Job...");
       TableResult insertResult = tableEnv.executeSql(
             "INSERT INTO users VALUES " +
                   "  (1, 'Alice', 30, 'New York')," +
                   "  (2, 'Bob', 25, 'San Francisco')," +
                   "  (3, 'Charlie', 35, 'Seattle')"
       );

       // Wait for the insert to finish
       insertResult.await();
       System.out.println("Insert finished successfully.");

       // ⚠️ IMPORTANT: In K8s Application Mode, the process usually exits here.
       // If you need to see the data, you should run a SECOND job or use a
       // SQL Client pod to query the Iceberg table.
       // tableEnv.executeSql("SELECT * FROM users").print(); // <--- REMOVE THIS
    }
}
