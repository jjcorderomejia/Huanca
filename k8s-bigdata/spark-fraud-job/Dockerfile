FROM ghcr.io/jjcorderomejia/spark:3.5.6-20260207032726

USER root

# Install Python dependencies
RUN pip3 install --no-cache-dir     starrocks==1.0.0     pyarrow==14.0.1     geopy==2.4.1

# Copy Spark jobs
COPY fraud_stream_to_starrocks.py /opt/spark/jobs/fraud_stream_to_starrocks.py
COPY customer_csv_to_iceberg.py   /opt/spark/jobs/customer_csv_to_iceberg.py
COPY compact_iceberg.py           /opt/spark/jobs/compact_iceberg.py
/
# StarRocks Spark connector JAR
ADD https://repo1.maven.org/maven2/com/starrocks/starrocks-spark-connector-3.x_2.12/1.1.2/starrocks-spark-connector-3.x_2.12-1.1.2.jar     /opt/spark/jars/starrocks-spark-connector.jar

# Iceberg + S3A JARs
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar     /opt/spark/jars/iceberg-spark-runtime.jar

ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar     /opt/spark/jars/hadoop-aws.jar

ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar     /opt/spark/jars/aws-java-sdk-bundle.jar

# PostgreSQL JDBC driver for Iceberg JDBC catalog
ADD https://jdbc.postgresql.org/download/postgresql-42.7.3.jar     /opt/spark/jars/postgresql.jar

USER 185
