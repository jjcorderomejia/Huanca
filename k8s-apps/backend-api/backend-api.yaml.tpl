apiVersion: apps/v1
kind: Deployment
metadata:
  name: backend-api
  namespace: apps
spec:
  replicas: 1
  selector:
    matchLabels:
      app: backend-api
  template:
    metadata:
      labels:
        app: backend-api
    spec:
      imagePullSecrets:
        - name: ghcr-creds
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
        fsGroup: 1000
      containers:
        - name: backend-api
          image: ghcr.io/${ORG}/fraud-api:${GIT_SHA}
          imagePullPolicy: Always
          ports:
            - containerPort: 8000
          env:
            - name: REDPANDA_BOOTSTRAP
              value: "fraud-redpanda-0.fraud-redpanda.bigdata.svc.cluster.local:9092"
            - name: STARROCKS_HOST
              value: "starrocks-fe.bigdata.svc.cluster.local"
            - name: STARROCKS_PORT
              value: "9030"
            - name: STARROCKS_DB
              value: "fraud"
            - name: STARROCKS_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: starrocks-credentials
                  key: root-password
            - name: TOPIC_RAW
              value: "transactions-raw"
            - name: API_KEY
              valueFrom:
                secretKeyRef:
                  name: api-key-secret
                  key: api-key
          resources:
            requests:
              cpu: "100m"
              memory: "256Mi"
            limits:
              cpu: "500m"
              memory: "512Mi"
          readinessProbe:
            httpGet:
              path: /api/health
              port: 8000
            initialDelaySeconds: 10
            periodSeconds: 5
            timeoutSeconds: 5
          livenessProbe:
            httpGet:
              path: /api/health
              port: 8000
            initialDelaySeconds: 30
            periodSeconds: 10
            timeoutSeconds: 5
            failureThreshold: 3
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
---
apiVersion: v1
kind: Service
metadata:
  name: backend-api
  namespace: apps
spec:
  selector:
    app: backend-api
  ports:
    - port: 8000
      targetPort: 8000
