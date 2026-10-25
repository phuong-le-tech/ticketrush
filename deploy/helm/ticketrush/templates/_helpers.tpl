{{- define "ticketrush.secretName" -}}
ticketrush-credentials
{{- end -}}

{{- define "ticketrush.kafkaBootstrap" -}}
{{ .Values.kafka.name }}-kafka-bootstrap:9092
{{- end -}}

{{/* Env var read from a key of the credentials Secret. Args: dict "name" "key" "root". */}}
{{- define "ticketrush.secretEnv" -}}
- name: {{ .name }}
  valueFrom:
    secretKeyRef:
      name: {{ include "ticketrush.secretName" .root }}
      key: {{ .key }}
{{- end -}}
