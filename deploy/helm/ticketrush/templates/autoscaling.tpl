{{- if .Values.autoscaling.enabled }}
{{- range $name, $topic := .Values.autoscaling.consumers }}
---
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: {{ $name }}
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: {{ $name }}
  minReplicaCount: 1
  maxReplicaCount: {{ $.Values.autoscaling.maxReplicas }}
  pollingInterval: {{ $.Values.autoscaling.pollingInterval }}
  cooldownPeriod: {{ $.Values.autoscaling.cooldownPeriod }}
  advanced:
    horizontalPodAutoscalerConfig:
      behavior:
        scaleDown:
          stabilizationWindowSeconds: 30
  triggers:
    - type: kafka
      metadata:
        bootstrapServers: {{ include "ticketrush.kafkaBootstrap" $ | quote }}
        consumerGroup: {{ $name | quote }}
        topic: {{ $topic | quote }}
        lagThreshold: {{ $.Values.autoscaling.lagThreshold | quote }}
        offsetResetPolicy: earliest
{{- end }}
{{- end }}
