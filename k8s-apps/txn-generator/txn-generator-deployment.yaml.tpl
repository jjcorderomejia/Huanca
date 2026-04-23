apiVersion: apps/v1
kind: Deployment
metadata:
  name: txn-generator
  namespace: apps
spec:
  replicas: 1
  selector:
    matchLabels:
      app: txn-generator
  template:
    metadata:
      labels:
        app: txn-generator
    spec:
      imagePullSecrets:
        - name: ghcr-creds
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
        fsGroup: 1000
      containers:
        - name: txn-generator
          image: ghcr.io/${ORG}/txn-generator:${GIT_SHA}
          imagePullPolicy: Always
          env:
            - name: BACKEND_URL
              value: "http://backend-api.apps.svc.cluster.local:8000"
            - name: API_KEY
              valueFrom:
                secretKeyRef:
                  name: api-key-secret
                  key: api-key
            - name: TXN_RATE
              value: "6.0"
            - name: BACKOFF_MAX
              value: "60"
          resources:
            requests:
              cpu: "50m"
              memory: "64Mi"
            limits:
              cpu: "200m"
              memory: "128Mi"
