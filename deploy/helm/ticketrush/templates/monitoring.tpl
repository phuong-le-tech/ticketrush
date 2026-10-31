{{- if .Values.monitoring.enabled }}
{{- range $name, $svc := .Values.services }}
---
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: {{ $name }}
  labels:
    monitoring: ticketrush
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: {{ $name }}
  endpoints:
    - port: http
      path: /actuator/prometheus
      interval: 5s
{{- end }}
---
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata:
  name: kafka-exporter
  labels:
    monitoring: ticketrush
spec:
  selector:
    matchLabels:
      strimzi.io/name: {{ .Values.kafka.name }}-kafka-exporter
  podMetricsEndpoints:
    - port: tcp-prometheus
      interval: 5s
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ticketrush-prometheus
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: ticketrush-prometheus
rules:
  - apiGroups: [""]
    resources: [services, endpoints, pods]
    verbs: [get, list, watch]
  - apiGroups: [discovery.k8s.io]
    resources: [endpointslices]
    verbs: [get, list, watch]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: ticketrush-prometheus
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: ticketrush-prometheus
subjects:
  - kind: ServiceAccount
    name: ticketrush-prometheus
    namespace: {{ .Release.Namespace }}
---
apiVersion: monitoring.coreos.com/v1
kind: Prometheus
metadata:
  name: ticketrush
spec:
  image: {{ .Values.monitoring.prometheusImage }}
  version: v3.15.1
  replicas: 1
  serviceAccountName: ticketrush-prometheus
  serviceMonitorSelector:
    matchLabels:
      monitoring: ticketrush
  podMonitorSelector:
    matchLabels:
      monitoring: ticketrush
  retention: 2h
  scrapeInterval: 5s
  evaluationInterval: 5s
  resources:
    requests: {cpu: 100m, memory: 192Mi}
    limits: {cpu: 500m, memory: 384Mi}
  securityContext:
    runAsNonRoot: true
    runAsUser: 65534
    fsGroup: 65534
    seccompProfile:
      type: RuntimeDefault
---
apiVersion: v1
kind: Service
metadata:
  name: prometheus
spec:
  selector:
    prometheus: ticketrush
  ports:
    - name: http
      port: 9090
      targetPort: 9090
{{- end }}
