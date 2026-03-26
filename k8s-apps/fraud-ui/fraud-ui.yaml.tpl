apiVersion: apps/v1
kind: Deployment
metadata:
  name: fraud-ui
  namespace: apps
spec:
  replicas: 1
  selector:
    matchLabels:
      app: fraud-ui
  template:
    metadata:
      labels:
        app: fraud-ui
    spec:
      securityContext:
        runAsNonRoot: true
        runAsUser: 101
        fsGroup: 101
      imagePullSecrets:
        - name: ghcr-creds
      containers:
        - name: fraud-ui
          image: ghcr.io/${ORG}/fraud-ui:${GIT_SHA}
          imagePullPolicy: Always
          env:
            - name: API_KEY
              valueFrom:
                secretKeyRef:
                  name: api-key-secret
                  key: api-key
          ports:
            - containerPort: 8080
          livenessProbe:
            httpGet:
              path: /
              port: 8080
            initialDelaySeconds: 5
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /
              port: 8080
            initialDelaySeconds: 3
            periodSeconds: 5
          resources:
            requests:
              cpu: "50m"
              memory: "64Mi"
            limits:
              cpu: "200m"
              memory: "128Mi"
---
apiVersion: v1
kind: Service
metadata:
  name: fraud-ui
  namespace: apps
spec:
  selector:
    app: fraud-ui
  ports:
    - port: 80
      targetPort: 8080
