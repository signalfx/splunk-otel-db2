{{/*
Expand the name of the chart.
*/}}
{{- define "splunk-otel-db2.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "splunk-otel-db2.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "splunk-otel-db2.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "splunk-otel-db2.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "splunk-otel-db2.selectorLabels" -}}
app.kubernetes.io/name: {{ include "splunk-otel-db2.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
ServiceAccount name
*/}}
{{- define "splunk-otel-db2.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "splunk-otel-db2.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Image tag — falls back to chart appVersion
*/}}
{{- define "splunk-otel-db2.imageTag" -}}
{{- .Values.image.tag | default .Chart.AppVersion }}
{{- end }}

{{/*
Name of the credentials secret for a receiver entry.
Uses credentialsSecret if set, otherwise the chart-created secret name.
*/}}
{{/*
Name of the credentials secret for a receiver entry.
Call with a dict: (dict "receiver" . "releaseName" $.Release.Name)
*/}}
{{- define "splunk-otel-db2.credentialsSecretName" -}}
{{- if .receiver.credentialsSecret }}
{{- .receiver.credentialsSecret }}
{{- else }}
{{- printf "%s-credentials-%s" .releaseName .receiver.name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
