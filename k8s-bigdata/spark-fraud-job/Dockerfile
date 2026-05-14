FROM ghcr.io/jjcorderomejia/spark:3.5.6-20260207032726

USER root

# Install pip and Python dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3-pip && \
    rm -rf /var/lib/apt/lists/* && \
    pip3 install --no-cache-dir "pyarrow>=16.0.0,<18.0.0" pandas

# Copy Spark jobs
COPY fraud_stream_to_starrocks.py      /opt/spark/jobs/fraud_stream_to_starrocks.py
COPY customer_csv_to_iceberg.py        /opt/spark/jobs/customer_csv_to_iceberg.py
COPY init_iceberg_schema.py            /opt/spark/jobs/init_iceberg_schema.py
COPY compact_iceberg.py                /opt/spark/jobs/compact_iceberg.py
COPY fraud_iceberg_expire.py           /opt/spark/jobs/fraud_iceberg_expire.py
COPY risk_profiles_to_iceberg.py       /opt/spark/jobs/risk_profiles_to_iceberg.py

# StarRocks Spark connector JAR
ADD https://github.com/StarRocks/starrocks-connector-for-apache-spark/releases/download/v1.1.3/starrocks-spark-connector-3.5_2.12-1.1.3.jar \
    /opt/spark/jars/starrocks-spark-connector.jar

# Iceberg + S3A JARs
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.2/iceberg-spark-runtime-3.5_2.12-1.7.2.jar \
    /opt/spark/jars/iceberg-spark-runtime.jar

ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    /opt/spark/jars/hadoop-aws.jar

ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    /opt/spark/jars/aws-java-sdk-bundle.jar

# PostgreSQL JDBC driver for Iceberg JDBC catalog
ADD https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
    /opt/spark/jars/postgresql.jar

# Docker ADD from URL creates files as root:root 600 — make all JARs readable by hadoop (UID 1000)
RUN chmod 644 /opt/spark/jars/*.jar

# Switch back to the Spark service user defined in the base image.
# The base image (ghcr.io/jjcorderomejia/spark:3.5.6-*) uses 'hadoop' (UID 1000, GID 1000).
# Numeric UID required — runAsNonRoot: true in K8s cannot verify named users at admission.
# hadoop owns /opt/spark (755) — the entrypoint WORKDIR where java_opts.txt is written.
# IMPORTANT: The SparkApplication manifest (spark-fraud-stream.yaml.tpl) sets:
#   podSecurityContext.runAsNonRoot: true  — K8s enforces non-root at admission
#   podSecurityContext.fsGroup: 1000       — matches hadoop GID for volume mounts
# If the base image ever changes this UID, update fsGroup in spark-fraud-stream.yaml.tpl
# to match. The two values must stay in sync.
USER 1000
