{{- if .Values.monitoring.enabled }}
{{- $existing := lookup "v1" "Secret" .Release.Namespace "ticketrush-grafana" }}
---
apiVersion: v1
kind: Secret
metadata:
  name: ticketrush-grafana
type: Opaque
data:
  admin-user: {{ .Values.monitoring.grafanaAdminUser | b64enc | quote }}
  {{- if .Values.monitoring.grafanaAdminPassword }}
  admin-password: {{ .Values.monitoring.grafanaAdminPassword | b64enc | quote }}
  {{- else if and $existing $existing.data }}
  admin-password: {{ index $existing.data "admin-password" | quote }}
  {{- else }}
  admin-password: {{ randAlphaNum 32 | b64enc | quote }}
  {{- end }}
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: ticketrush-dashboard
  labels:
    grafana_dashboard: "1"
data:
  ticketrush.json: |-
    {{- .Files.Get "dashboards/ticketrush.json" | nindent 4 }}
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: ticketrush-grafana-provisioning
data:
  dashboards.yaml: |
    apiVersion: 1
    providers:
      - name: ticketrush
        orgId: 1
        type: file
        disableDeletion: true
        updateIntervalSeconds: 10
        options:
          path: /var/lib/grafana/dashboards
  datasource.yaml: |
    apiVersion: 1
    datasources:
      - name: prometheus
        uid: prometheus
        type: prometheus
        access: proxy
        url: http://prometheus:9090
        isDefault: true
        editable: false
---
apiVersion: v1
kind: Service
metadata:
  name: grafana
spec:
  selector:
    app.kubernetes.io/name: grafana
  ports:
    - name: http
      port: 80
      targetPort: http
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: grafana
spec:
  serviceName: grafana
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: grafana
  template:
    metadata:
      labels:
        app.kubernetes.io/name: grafana
    spec:
      automountServiceAccountToken: false
      securityContext:
        runAsNonRoot: true
        runAsUser: 472
        fsGroup: 472
        seccompProfile:
          type: RuntimeDefault
      containers:
        - name: grafana
          image: {{ .Values.monitoring.grafanaImage }}
          ports:
            - name: http
              containerPort: 3000
          env:
            - name: GF_SECURITY_ADMIN_USER
              valueFrom:
                secretKeyRef:
                  name: ticketrush-grafana
                  key: admin-user
            - name: GF_SECURITY_ADMIN_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: ticketrush-grafana
                  key: admin-password
          resources:
            requests: {cpu: 100m, memory: 256Mi}
            limits: {cpu: "1", memory: 512Mi}
          startupProbe:
            httpGet: {path: /api/health, port: http}
            failureThreshold: 60
            periodSeconds: 5
          readinessProbe:
            httpGet: {path: /api/health, port: http}
          livenessProbe:
            httpGet: {path: /api/health, port: http}
            initialDelaySeconds: 30
          securityContext:
            allowPrivilegeEscalation: false
            capabilities:
              drop: ["ALL"]
          volumeMounts:
            - name: storage
              mountPath: /var/lib/grafana
            - name: dashboard
              mountPath: /var/lib/grafana/dashboards
            - name: providers
              mountPath: /etc/grafana/provisioning/dashboards
            - name: datasources
              mountPath: /etc/grafana/provisioning/datasources
      volumes:
        - name: storage
          emptyDir: {}
        - name: dashboard
          configMap:
            name: ticketrush-dashboard
        - name: providers
          configMap:
            name: ticketrush-grafana-provisioning
            items:
              - key: dashboards.yaml
                path: dashboards.yaml
        - name: datasources
          configMap:
            name: ticketrush-grafana-provisioning
            items:
              - key: datasource.yaml
                path: datasource.yaml
{{- end }}
