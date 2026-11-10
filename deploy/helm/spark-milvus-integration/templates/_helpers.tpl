{{/* Chart name. */}}
{{- define "spark-milvus-integration.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Release-scoped resource name. */}}
{{- define "spark-milvus-integration.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := include "spark-milvus-integration.name" . }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/* Release labels shared by every resource. */}}
{{- define "spark-milvus-integration.commonLabels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/name: {{ include "spark-milvus-integration.name" . }}
app.kubernetes.io/instance: {{ .Release.Name | quote }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service | quote }}
{{- end }}

{{- define "spark-milvus-integration.labels" -}}
{{ include "spark-milvus-integration.commonLabels" . }}
app.kubernetes.io/component: integration-test
{{- end }}

{{/* The fixed-input reader receives no service or write configuration. */}}
{{- define "spark-milvus-integration.runnerEnv" -}}
- name: CI_RUN_ID
  value: {{ default .Release.Name .Values.runId | quote }}
- name: MILVUS_READ_IT_OPTIONS
  value: /etc/milvus-read/a.json
- name: MILVUS_READ_IT_OPTIONS_B
  value: /etc/milvus-read/b.json
{{- end }}
