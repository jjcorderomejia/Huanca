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
  imagePullPolicy: Always
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
    # Redpanda compatibility: force consumer-based offset fetching.
    # Spark 3.4+ KafkaOffsetReaderAdmin uses AdminClient.describeTopics which
    # times out against Redpanda — the deprecated consumer path works correctly.
    "spark.sql.streaming.kafka.useDeprecatedOffsetFetching": "true"
    # RocksDB state store: avoids HDFSBacked rename race on S3A (caused 1.delta
    # FileNotFoundException incident 2026-05-13). State lives on local executor
    # disk; checkpointed to S3 in bulk on commit.
    "spark.sql.streaming.stateStore.providerClass": "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"
    # Event log to MinIO with rolling — item #1 of S7-observability-debt.md.
    # Without this, post-mortems for executor crashes have no per-executor
    # memory-pool history (Spark History Server has no data to render).
    # Rolling at 128 MB caps individual file size; MinIO bucket lifecycle
    # (configured separately via mc ilm rule add) handles retention.
    "spark.eventLog.enabled": "true"
    "spark.eventLog.dir": "s3a://checkpoints/spark-events"
    "spark.eventLog.rolling.enabled": "true"
    "spark.eventLog.rolling.maxFileSize": "128m"
    # Native memory caps (S7-streaming-executor-oom-sigkill.md). The default JVM
    # has NO upper bound on direct memory, metaspace, or code cache — combined
    # with -Xms=-Xmx (Spark pre-commits the full heap), the pod is structurally
    # set up to exceed its cgroup limit. These caps make any genuine native leak
    # throw a Java OOM with a stack trace naming the pool, instead of a silent
    # kernel SIGKILL with no diagnostic signal.
    # Budget: 1g direct + 512m metaspace + 256m code cache + ~500m Python workers
    # + ~250m JVM bookkeeping ≈ 2.5g native. Pairs with memoryOverhead: "4g".
    "spark.executor.extraJavaOptions": "-XX:MaxDirectMemorySize=1g -XX:MaxMetaspaceSize=512m -XX:ReservedCodeCacheSize=256m -XX:+ExitOnOutOfMemoryError -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp"

  driver:
    cores: 1
    coreRequest: "500m"
    coreLimit: "1200m"
    memory: "2g"
    serviceAccount: spark
    # runAsNonRoot enforces non-root at K8s admission without pinning the UID.
    # fsGroup: 1000 matches hadoop GID in the base image (ghcr.io/jjcorderomejia/spark:3.5.6-*).
    # If the base image ever changes this GID, update fsGroup here AND in the executor section below.
    podSecurityContext:
      runAsNonRoot: true
      fsGroup: 1000
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
      - name: TOPIC_SCORED
        value: "transactions-scored"
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
      - name: CUSTOMER_ICEBERG_TABLE
        value: "iceberg.fraud.customers"
      - name: CUSTOMER_CACHE_TTL_SEC
        value: "3600"
      - name: STARTING_OFFSETS
        value: "earliest"
      - name: MAX_OFFSETS_PER_TRIGGER
        value: "50000"
      - name: CHECKPOINT_LOCATION
        value: "s3a://checkpoints/fraud-stream-v2"
    volumeMounts:
      - name: redpanda-certs
        mountPath: /etc/redpanda-certs
        readOnly: true

  executor:
    instances: 1
    cores: 1
    coreRequest: "500m"
    memory: "5g"
    memoryOverhead: "4g"
    # Pod stays at 9g total. Rebalanced 8g+1g → 5g+4g after S7 forensics
    # (huanca/ops/S7-streaming-executor-oom-sigkill.md): measured peak heap 1.7g,
    # native floor ~2g (Python workers + Arrow IPC + Kafka + Netty + RocksDB JNI
    # + metaspace). 1g overhead was structurally insufficient — guaranteed cgroup
    # SIGKILL within 10-17h. The extraJavaOptions caps in sparkConf above convert
    # any genuine native leak into a loud Java OOM with stack trace, instead of a
    # silent kernel SIGKILL that burns through executionAttempts.
    # Prior 4g→8g bump (2026-04 incident) doubled the wrong budget; native was
    # the starved pool. Lesson: heap usage (jstat -gc) is the load-bearing signal.
    # coreRequest 500m decouples K8s scheduler reservation from Spark logical cores;
    # under burst, K8s may throttle to 0.5 actual CPU — acceptable, does not OOM.
    # fsGroup must match driver — both use hadoop GID 1000 from the same base image.
    podSecurityContext:
      runAsNonRoot: true
      fsGroup: 1000
    env:
      - name: REDPANDA_BOOTSTRAP
        value: "fraud-redpanda-0.fraud-redpanda.bigdata.svc.cluster.local:9092"
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
      - name: redpanda-certs
        mountPath: /etc/redpanda-certs
        readOnly: true

  volumes:
    - name: redpanda-certs
      secret:
        secretName: fraud-redpanda-default-root-certificate
        items:
          - key: ca.crt
            path: ca.crt

  restartPolicy:
    type: OnFailure
    onFailureRetries: 3
    onFailureRetryInterval: 10
    onSubmissionFailureRetries: 5
    onSubmissionFailureRetryInterval: 20
