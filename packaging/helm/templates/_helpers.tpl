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
Validate every receiver name.
  - Must match ^[a-z][a-z0-9-]*$ (lowercase DNS-compatible: letters, digits, hyphens; starts with letter).
  - Underscores and uppercase are rejected so that names like PROD_EU or foo_bar cannot
    be used. Both foo-bar and foo_bar normalise to FOO_BAR in env-var names, which would
    silently wire two receivers to the same Secret values.
  - After normalisation (upper + replace "-" "_") duplicates are caught explicitly.
Call via: {{ include "splunk-otel-db2.validateReceiverNames" . }}
*/}}
{{- define "splunk-otel-db2.validateReceiverNames" -}}
{{- $seen := dict -}}
{{- range .Values.receivers -}}
  {{- if not (mustRegexMatch "^[a-z][a-z0-9-]*$" .name) -}}
    {{- fail (printf "receivers[%s]: name must be lowercase DNS-compatible (a-z, 0-9, hyphens only, must start with a letter; no uppercase or underscores)" .name) -}}
  {{- end -}}
  {{- $envKey := .name | upper | replace "-" "_" -}}
  {{- if hasKey $seen $envKey -}}
    {{- fail (printf "receivers[%s]: normalised env-var key %s collides with receiver '%s'" .name $envKey (index $seen $envKey)) -}}
  {{- end -}}
  {{- $_ := set $seen $envKey .name -}}
{{- end -}}
{{- end }}

{{/*
Name of the credentials secret for a receiver entry.
Call with a dict: (dict "receiver" . "releaseName" $.Release.Name)
*/}}
{{- define "splunk-otel-db2.credentialsSecretName" -}}
{{- if .receiver.credentialsSecret }}
{{- .receiver.credentialsSecret }}
{{- else }}
{{- $full := printf "%s-credentials-%s" .releaseName .receiver.name }}
{{- if le (len $full) 63 }}
{{- $full }}
{{- else }}
{{- $hash := $full | sha256sum | trunc 6 }}
{{- printf "%s-credentials-%s" .releaseName .receiver.name | trunc 55 | trimSuffix "-" }}-{{ $hash }}
{{- end }}
{{- end }}
{{- end }}
