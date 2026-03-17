apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: spark-fraud-stream
  namespace: bigdata
spec:
  type: Python
  mode: cluster
  sparkVersion: "3.5.6"
  image: ghcr.io/${ORG}/fraud-spark-job:${GIT_SHA}
  imagePullPolicy: IfNotPresent
  imagePullSecrets:
    - ghcr-creds

  mainApplicationFile: local:///opt/spark/jobs/fraud_stream_to_starrocks.py

  sparkConf:
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog"
    "spark.sql.catalog.iceberg.type": "jdbc"
    "spark.sql.catalog.iceberg.uri": "jdbc:postgresql://airflow-postgresql.bigdata.svc.cluster.local:5432/iceberg_catalog"
    "spark.sql.catalog.iceberg.jdbc.user": "postgres"
    "spark.sql.catalog.iceberg.warehouse": "s3a://iceberg/warehouse"
    "spark.hadoop.fs.s3a.endpoint": "http://minio.bigdata.svc.cluster.local:9000"
    "spark.hadoop.fs.s3a.path.style.access": "true"
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
    "spark.sql.shuffle.partitions": "48"

  driver:
    cores: 1
    coreLimit: "1200m"
    memory: "2g"
    serviceAccount: spark
    env:
      - name: REDPANDA_BOOTSTRAP
        value: "fraud-redpanda-0.fraud-redpanda.bigdata.svc.cluster.local:9092"
      - name: TOPIC_IN
        value: "transactions-raw"
      - name: TOPIC_DLQ
        value: "transactions-dlq"
      - name: TOPIC_CLEAN
        value: "transactions-clean"
      - name: TOPIC_ALERTS
        value: "fraud-alerts"
      - name: STARROCKS_HOST
        value: "starrocks-fe-svc.bigdata.svc.cluster.local"
      - name: STARROCKS_PORT
        value: "9030"
      - name: STARROCKS_DB
        value: "fraud"
      - name: STARROCKS_USER
        value: "root"
      - name: STARROCKS_PASSWORD
        valueFrom:
          secretKeyRef:
            name: starrocks-credentials
            key: root-password
      - name: MINIO_ACCESS_KEY
        valueFrom:
          secretKeyRef:
            name: minio-secret
            key: MINIO_ROOT_USER
      - name: MINIO_SECRET_KEY
        valueFrom:
          secretKeyRef:
            name: minio-secret
            key: MINIO_ROOT_PASSWORD
      - name: ICEBERG_DB_PASSWORD
        valueFrom:
          secretKeyRef:
            name: airflow-postgresql
            key: postgres-password
      - name: CUSTOMER_CSV_PATH
        value: "/opt/enrichment/customer.csv"
      - name: STARTING_OFFSETS
        value: "latest"
      - name: CHECKPOINT_LOCATION
        value: "s3a://checkpoints/fraud-stream"
    volumeMounts:
      - name: enrichment
        mountPath: /opt/enrichment

  executor:
    instances: 2
    cores: 2
    memory: "3g"
    env:
      - name: REDPANDA_BOOTSTRAP
        value: "fraud-redpanda-0.fraud-redpanda.bigdata.svc.cluster.local:9092"
      - name: CUSTOMER_CSV_PATH
        value: "/opt/enrichment/customer.csv"
      - name: MINIO_ACCESS_KEY
        valueFrom:
          secretKeyRef:
            name: minio-secret
            key: MINIO_ROOT_USER
      - name: MINIO_SECRET_KEY
        valueFrom:
          secretKeyRef:
            name: minio-secret
            key: MINIO_ROOT_PASSWORD
      - name: STARROCKS_PASSWORD
        valueFrom:
          secretKeyRef:
            name: starrocks-credentials
            key: root-password
      - name: ICEBERG_DB_PASSWORD
        valueFrom:
          secretKeyRef:
            name: airflow-postgresql
            key: postgres-password
    volumeMounts:
      - name: enrichment
        mountPath: /opt/enrichment

  volumes:
    - name: enrichment
      configMap:
        name: customer-csv

  restartPolicy:
    type: OnFailure
    onFailureRetries: 3
    onFailureRetryInterval: 10
    onSubmissionFailureRetries: 5
    onSubmissionFailureRetryInterval: 20
