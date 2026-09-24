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

{{- define "spark-milvus-integration.milvusName" -}}
{{ printf "%s-milvus" (include "spark-milvus-integration.fullname" . | trunc 56 | trimSuffix "-") }}
{{- end }}

{{- define "spark-milvus-integration.milvusUri" -}}
{{- if .Values.milvus.deployment.enabled -}}
{{ printf "http://%s:19530" (include "spark-milvus-integration.milvusName" .) }}
{{- else -}}
{{ required "milvus.uri is required for an existing instance" .Values.milvus.uri }}
{{- end -}}
{{- end }}

{{- define "spark-milvus-integration.storageRoot" -}}
{{- if .Values.milvus.deployment.enabled -}}
{{ printf "%s/ci/%s/%s/%s" .Values.objectStorage.rootPath .Release.Namespace .Release.Name (default .Release.Name .Values.runId) }}
{{- else -}}
{{ .Values.objectStorage.rootPath }}
{{- end -}}
{{- end }}

{{/* Same configuration for private fixture preparation and the test runner. */}}
{{- define "spark-milvus-integration.runnerEnv" -}}
- name: CI_RUN_ID
  value: {{ default .Release.Name .Values.runId | quote }}
- name: MILVUS_UAT_URI
  value: {{ include "spark-milvus-integration.milvusUri" . | quote }}
- name: MILVUS_UAT_DATABASE
  value: {{ .Values.milvus.database | quote }}
- name: MILVUS_UAT_TOKEN
  valueFrom:
    secretKeyRef:
      name: {{ .Values.milvus.credentials.existingSecret | quote }}
      key: {{ .Values.milvus.credentials.tokenKey | quote }}
- name: MILVUS_JNI_S3_ENDPOINT
  value: {{ .Values.objectStorage.endpoint | quote }}
- name: MILVUS_JNI_S3_BUCKET
  value: {{ .Values.objectStorage.bucket | quote }}
- name: MILVUS_JNI_S3_ROOT_PATH
  value: {{ include "spark-milvus-integration.storageRoot" . | quote }}
- name: MILVUS_JNI_S3_REGION
  value: {{ .Values.objectStorage.region | quote }}
- name: MILVUS_JNI_S3_USE_SSL
  value: {{ .Values.objectStorage.useSSL | quote }}
- name: MILVUS_JNI_S3_USE_VIRTUAL_HOST
  value: {{ .Values.objectStorage.useVirtualHost | quote }}
- name: MILVUS_UAT_WRITE_PREFIX
  value: {{ printf "%s/%s" (trimSuffix "/" .Values.objectStorage.writePrefix) (default .Release.Name .Values.runId) | quote }}
- name: AWS_REGION
  value: {{ .Values.objectStorage.region | quote }}
- name: AWS_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef:
      name: {{ .Values.objectStorage.credentials.existingSecret | quote }}
      key: {{ .Values.objectStorage.credentials.accessKeyIdKey | quote }}
- name: AWS_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ .Values.objectStorage.credentials.existingSecret | quote }}
      key: {{ .Values.objectStorage.credentials.secretAccessKeyKey | quote }}
{{- end }}
