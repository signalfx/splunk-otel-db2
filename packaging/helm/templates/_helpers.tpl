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
OTLP exporter endpoint.

An explicit otlp.endpoint always wins. Otherwise the endpoint is derived, which is only
possible when this chart is installed as a dependency of splunk-otel-collector: the collector
Services are named from the parent release, so the name cannot be known standalone.

Target selection cannot read the parent's top-level `gateway.enabled`. Helm exposes parent
values to a subchart only through `global`, so a parent that sets `gateway.enabled: true` at
its top level is invisible here. Honour `global.gateway.enabled` when the parent chooses to
pass it down, and otherwise fall back to this chart's own otlp.collectorTarget.

The agent Service is `internalTrafficPolicy: Local`, so it only resolves to an agent Pod on
the same Node. That is the default because the agent DaemonSet is always enabled upstream
while the gateway is not, but it means exports blackhole if this Pod's Node has no healthy
agent. Prefer the gateway when it is deployed.
*/}}
{{- define "splunk-otel-db2.otlpEndpoint" -}}
{{- if .Values.otlp.endpoint -}}
{{- .Values.otlp.endpoint -}}
{{- else if .Chart.IsRoot -}}
{{- fail "otlp.endpoint is required when installing splunk-otel-db2 standalone; it can only be derived when the chart runs as a dependency of splunk-otel-collector" -}}
{{- else -}}
{{- $target := .Values.otlp.collectorTarget | default "agent" -}}
{{- if dig "gateway" "enabled" false (.Values.global | default dict) -}}
{{- $target = "gateway" -}}
{{- end -}}
{{- if eq $target "gateway" -}}
http://{{ .Release.Name }}-splunk-otel-collector:4317
{{- else if eq $target "agent" -}}
http://{{ .Release.Name }}-splunk-otel-collector-agent:4317
{{- else -}}
{{- fail (printf "otlp.collectorTarget must be \"agent\" or \"gateway\", got %q" $target) -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/*
Validate every receiver name.
  - Must match ^[a-z]([a-z0-9-]*[a-z0-9])?$ (lowercase DNS-compatible: letters, digits,
    interior hyphens; must start and end with an alphanumeric character).
  - Trailing hyphens are rejected: "prod-" would produce a Kubernetes Secret whose name
    ends in "-", which the Kubernetes API rejects.
  - Underscores and uppercase are rejected so that names like PROD_EU or foo_bar cannot
    be used. Both foo-bar and foo_bar normalise to FOO_BAR in env-var names, which would
    silently wire two receivers to the same Secret values.
  - After normalisation (upper + replace "-" "_") duplicates are caught explicitly.
Call via: {{ include "splunk-otel-db2.validateReceiverNames" . }}
*/}}
{{- define "splunk-otel-db2.validateReceiverNames" -}}
{{- $seen := dict -}}
{{- range .Values.receivers -}}
  {{- if not (mustRegexMatch "^[a-z]([a-z0-9-]*[a-z0-9])?$" .name) -}}
    {{- fail (printf "receivers[%s]: name must be lowercase DNS-compatible (a-z, 0-9, interior hyphens only; must start and end with an alphanumeric character)" .name) -}}
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
