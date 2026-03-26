apiVersion: batch/v1
kind: Job
metadata:
  name: buildkit-fraud-ui-${GIT_SHA}
  namespace: bigdata
spec:
  backoffLimit: 1
  template:
    spec:
      restartPolicy: Never
      serviceAccountName: buildkit
      imagePullSecrets:
        - name: ghcr-creds
      initContainers:
        - name: copy-context
          image: busybox:1.36
          command: ['cp', '-r', '/src/.', '/workspace/']
          volumeMounts:
            - name: src
              mountPath: /src
            - name: workspace
              mountPath: /workspace
      containers:
        - name: buildctl
          image: moby/buildkit:v0.13.2
          command:
            - buildctl
            - --addr
            - tcp://buildkitd.bigdata.svc.cluster.local:1234
            - build
            - --frontend=dockerfile.v0
            - --local
            - context=/workspace
            - --local
            - dockerfile=/workspace
            - --output
            - type=image,name=ghcr.io/${ORG}/fraud-ui:${GIT_SHA},push=true
          volumeMounts:
            - name: workspace
              mountPath: /workspace
            - name: ghcr-secret
              mountPath: /kaniko/.docker
              readOnly: true
          env:
            - name: DOCKER_CONFIG
              value: /kaniko/.docker
      volumes:
        - name: workspace
          emptyDir: {}
        - name: src
          hostPath:
            path: ${HOST_UI_ROOT}
            type: Directory
        - name: ghcr-secret
          secret:
            secretName: ghcr-creds
            items:
              - key: .dockerconfigjson
                path: config.json
