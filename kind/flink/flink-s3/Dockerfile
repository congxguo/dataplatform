FROM flink:1.20.3

# 1. Standard Iceberg Runtime for Flink 1.19 (Compatible with 1.20)
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.19/1.6.1/iceberg-flink-runtime-1.19-1.6.1.jar

# 2. Individual Hadoop Jars (The reliable way to satisfy HadoopUtils)
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.6/hadoop-common-3.3.6.jar
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-auth/3.3.6/hadoop-auth-3.3.6.jar
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-hdfs/3.3.6/hadoop-hdfs-3.3.6.jar
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-hdfs-client/3.3.6/hadoop-hdfs-client-3.3.6.jar
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/commons-configuration/commons-configuration/1.10/commons-configuration-1.10.jar
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/commons-collections/commons-collections/3.2.2/commons-collections-3.2.2.jar

# 3. AWS Bundle for S3FileIO
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.20.160/bundle-2.20.160.jar


# UGI's static block uses these for group mapping and configuration
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/commons-beanutils/commons-beanutils/1.9.4/commons-beanutils-1.9.4.jar


ADD flink-s3-fs-hadoop-1.20.0.jar /opt/flink/lib/
