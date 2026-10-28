{{- define "spark-milvus-demo.fullname" -}}
{{- printf "%s-%s" (.Release.Name | trunc 36 | trimSuffix "-") (.Release.Name | sha256sum | trunc 8) -}}
{{- end -}}

{{- define "spark-milvus-demo.labels" -}}
app.kubernetes.io/name: spark-milvus-demo
app.kubernetes.io/instance: {{ .Release.Name | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service | quote }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | quote }}
{{- end -}}

{{- define "spark-milvus-demo.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository .Values.image.tag -}}
{{- end -}}
{{- end -}}

{{- define "spark-milvus-demo.account" -}}
{{- $account := index .root.Values.serviceAccounts .role -}}
{{- if $account.name -}}
{{- $account.name -}}
{{- else if $account.create -}}
{{- printf "%s-%s" (include "spark-milvus-demo.fullname" .root) .role -}}
{{- else -}}
{{- fail (printf "serviceAccounts.%s.name is required when create=false" .role) -}}
{{- end -}}
{{- end -}}

{{- define "spark-milvus-demo.podSecurity" -}}
runAsNonRoot: true
runAsUser: 185
runAsGroup: 185
fsGroup: 185
seccompProfile:
  type: RuntimeDefault
{{- end -}}

{{- define "spark-milvus-demo.containerSecurity" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities:
  drop: ["ALL"]
{{- end -}}

{{- define "spark-milvus-demo.writableVolumes" -}}
- name: work-dir
  emptyDir: {}
- name: tmp
  emptyDir: {}
{{- end -}}

{{- define "spark-milvus-demo.writableMounts" -}}
- name: work-dir
  mountPath: /opt/spark/work-dir
- name: tmp
  mountPath: /tmp
{{- end -}}

{{- define "spark-milvus-demo.podTemplate" -}}
apiVersion: v1
kind: Pod
metadata:
  labels:
    {{- include "spark-milvus-demo.labels" .root | nindent 4 }}
spec:
  automountServiceAccountToken: {{ eq .role "driver" }}
  securityContext:
    {{- include "spark-milvus-demo.podSecurity" .root | nindent 4 }}
  containers:
    - name: spark-main
      securityContext:
        {{- include "spark-milvus-demo.containerSecurity" .root | nindent 8 }}
      volumeMounts:
        {{- include "spark-milvus-demo.writableMounts" .root | nindent 8 }}
  volumes:
    {{- include "spark-milvus-demo.writableVolumes" .root | nindent 4 }}
{{- end -}}

{{- define "spark-milvus-demo.arguments" -}}
{{- $name := include "spark-milvus-demo.fullname" . -}}
{{- $search := has .Values.example (list "exact" "filtered" "index") -}}
{{- if and (ne .Values.example "vectors") (empty .Values.input.existingSecret) -}}
{{- fail "input.existingSecret is required for snapshot examples" -}}
{{- end -}}
{{- $conf := dict
  "spark.kubernetes.namespace" .Release.Namespace
  "spark.kubernetes.container.image" (include "spark-milvus-demo.image" .)
  "spark.kubernetes.container.image.pullPolicy" .Values.image.pullPolicy
  "spark.kubernetes.authenticate.submission.oauthTokenFile" "/var/run/secrets/kubernetes.io/serviceaccount/token"
  "spark.kubernetes.authenticate.submission.caCertFile" "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
  "spark.kubernetes.authenticate.driver.serviceAccountName" (include "spark-milvus-demo.account" (dict "root" . "role" "driver"))
  "spark.kubernetes.authenticate.executor.serviceAccountName" (include "spark-milvus-demo.account" (dict "root" . "role" "executor"))
  "spark.kubernetes.driver.pod.name" (printf "%s-driver" $name)
  "spark.kubernetes.driver.podTemplateFile" "/etc/spark-milvus/templates/driver.yaml"
  "spark.kubernetes.executor.podTemplateFile" "/etc/spark-milvus/templates/executor.yaml"
  "spark.kubernetes.driver.podTemplateContainerName" "spark-main"
  "spark.kubernetes.executor.podTemplateContainerName" "spark-main"
  "spark.kubernetes.driver.ownPersistentVolumeClaim" "false"
  "spark.kubernetes.driver.reusePersistentVolumeClaim" "false"
  "spark.kubernetes.submission.waitAppCompletion" "true"
  "spark.dynamicAllocation.enabled" "false"
  "spark.sql.extensions" "com.zilliz.spark.connector.extensions.MilvusReadSparkSessionExtensions"
  "spark.plugins" "com.zilliz.spark.connector.extensions.MilvusSparkPlugin"
  "spark.pyspark.python" "/usr/bin/python3"
  "spark.pyspark.driver.python" "/usr/bin/python3"
  "spark.driver.memory" .Values.spark.driver.memory
  "spark.driver.memoryOverhead" .Values.spark.driver.memoryOverhead
  "spark.driver.cores" (toString .Values.spark.driver.cores)
  "spark.executor.memory" .Values.spark.executor.memory
  "spark.executor.memoryOverhead" .Values.spark.executor.memoryOverhead
  "spark.executor.cores" (toString .Values.spark.executor.cores)
  "spark.executor.instances" (toString .Values.spark.executor.instances)
  "spark.driver.extraJavaOptions" "-Djava.io.tmpdir=/tmp"
  "spark.executor.extraJavaOptions" "-Djava.io.tmpdir=/tmp"
  "spark.redaction.regex" "(?i)secret|password|token|access[._]?key"
  "spark.kubernetes.driver.label.app.kubernetes.io/instance" .Release.Name
  "spark.kubernetes.executor.label.app.kubernetes.io/instance" .Release.Name
-}}
{{- range $key, $value := .Values.nodeSelector -}}
{{- $_ := set $conf (printf "spark.kubernetes.node.selector.%s" $key) $value -}}
{{- end -}}
{{- if .Values.image.pullSecrets -}}
{{- $_ := set $conf "spark.kubernetes.container.image.pullSecrets" (join "," .Values.image.pullSecrets) -}}
{{- end -}}
{{- range $scope := list "spark.kubernetes.driverEnv" "spark.executorEnv" -}}
{{- range $key, $value := dict "LD_PRELOAD" "/opt/java/openjdk/lib/libjsig.so" "LD_BIND_NOW" "1" "MALLOC_ARENA_MAX" "2" "MALLOC_MMAP_THRESHOLD_" "1048576" "MALLOC_TRIM_THRESHOLD_" "134217728" -}}
{{- $_ := set $conf (printf "%s.%s" $scope $key) $value -}}
{{- end -}}
{{- end -}}
{{- if ne .Values.example "vectors" -}}
{{- $_ := set $conf (printf "spark.kubernetes.driver.secrets.%s" .Values.input.existingSecret) "/etc/spark-milvus/input" -}}
{{- end -}}
{{- range $key, $value := .Values.extraSparkConf -}}
{{- if hasKey $conf $key -}}
{{- fail (printf "extraSparkConf cannot override chart-managed setting %s" $key) -}}
{{- end -}}
{{- $_ := set $conf $key $value -}}
{{- end -}}
- /opt/spark/bin/spark-submit
- --master
- k8s://https://kubernetes.default.svc:443
- --deploy-mode
- cluster
- --name
- {{ $name | quote }}
{{- range $key := keys $conf | sortAlpha }}
- --conf
- {{ printf "%s=%s" $key (index $conf $key) | quote }}
{{- end }}
- --jars
- local:///opt/spark-milvus/connector.jar
{{- if eq .Values.language "python" }}
{{- $examples := dict "read" "01_read_snapshot.py" "filter" "02_filter_project.py" "sql" "03_sql_analytics.py" "exact" "04_exact_search.py" "filtered" "05_filtered_search.py" "index" "06_index_search.py" "vectors" "07_vector_functions.py" "partition" "08_partition_selection.py" }}
- --py-files
- local:///opt/spark-milvus/demo/python/common.py
- {{ printf "local:///opt/spark-milvus/demo/python/%s" (index $examples .Values.example) | quote }}
{{- else }}
{{- $examples := dict "read" "ReadSnapshot" "filter" "FilterProject" "sql" "SqlAnalytics" "exact" "ExactSearch" "filtered" "FilteredSearch" "index" "IndexSearch" "vectors" "VectorFunctions" "partition" "PartitionSelection" }}
- --class
- {{ printf "com.zilliz.spark.connector.demo.%s" (index $examples .Values.example) | quote }}
- local:///opt/spark-milvus/demo-java.jar
{{- end }}
{{- if ne .Values.example "vectors" }}
- --options
- {{ printf "/etc/spark-milvus/input/%s" .Values.input.optionsKey | quote }}
{{- end }}
{{- if $search }}
- --queries
{{- if .Values.input.queriesKey }}
- {{ printf "/etc/spark-milvus/input/%s" .Values.input.queriesKey | quote }}
{{- else }}
- /opt/spark-milvus/demo/config/queries.json
{{- end }}
{{- end }}
{{- end -}}
