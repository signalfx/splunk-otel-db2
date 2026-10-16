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
the same Node. That is the default because the agent DaemonSet is enabled upstream by
default while the gateway is not, but it means exports blackhole if this Pod's Node has no
healthy agent. Prefer the gateway when it is deployed.

The agent Service is also not always rendered: the parent gates it on
`agent.service.enabled`, and never creates it on EKS Fargate, which cannot run DaemonSets.
Those configurations need `otlp.collectorTarget: gateway` or an explicit otlp.endpoint,
and cannot be detected from here because the gating values are parent top-level values.

Addresses are emitted as cluster FQDNs rather than short names so they do not depend on the
Pod's resolv.conf search order and can cross namespaces. The namespace defaults to this
release's, which is wrong when the parent sets namespaceOverride — another top-level value
invisible here — so otlp.collectorNamespace exists to correct it without having to write
out the whole endpoint.
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
{{- $fullname := include "splunk-otel-db2.collectorFullname" . -}}
{{- $ns := .Values.otlp.collectorNamespace | default .Release.Namespace -}}
{{- if eq $target "gateway" -}}
http://{{ $fullname }}.{{ $ns }}.svc.cluster.local:4317
{{- else if eq $target "agent" -}}
http://{{ $fullname }}-agent.{{ $ns }}.svc.cluster.local:4317
{{- else -}}
{{- fail (printf "otlp.collectorTarget must be \"agent\" or \"gateway\", got %q" $target) -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/*
Reproduce splunk-otel-collector.fullname so derived Service names match the parent's.

That helper collapses the chart-name suffix when the release name already contains it, so
release "my-splunk-otel-collector" yields "my-splunk-otel-collector", not
"my-splunk-otel-collector-splunk-otel-collector". Appending the suffix unconditionally
produces a Service name that does not exist — and "my-splunk-otel-collector" is the release
name in the collector chart's own documented install command.

This cannot honour the parent's fullnameOverride or nameOverride, because Helm exposes
parent values to a subchart only through `global` and those keys live at the parent's top
level. A release using either must set otlp.endpoint explicitly.
*/}}
{{- define "splunk-otel-db2.collectorFullname" -}}
{{- $name := "splunk-otel-collector" -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
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
{{/*
Validate that receivers are configured at all, and that each has the fields with no sane
default. Messages name the full values path because this chart usually runs as a subchart,
where a bare field name gives the reader nothing to search for — and because a failure here
aborts the whole parent render, not just this subchart.

Helm replaces a list wholesale rather than merging its elements, so a user-supplied entry
inherits nothing from the default entry. Anything genuinely optional is therefore read with
`dig` and a default in the templates, and only what cannot be guessed is required here.
*/}}
{{- define "splunk-otel-db2.validateReceivers" -}}
{{- $prefix := .Chart.IsRoot | ternary "receivers" "db2.receivers" -}}
{{- if not (kindIs "slice" .Values.receivers) -}}
{{- if .Values.receivers -}}
{{- fail (printf "%s must be a list of receiver entries, got %s. A single entry still needs to be a one-item list." $prefix (kindOf .Values.receivers)) -}}
{{- end -}}
{{- end -}}
{{- if not .Values.receivers -}}
{{- fail (printf "%s is empty. Enabling this chart is not enough on its own: at least one receiver must be configured, each with name, hostname, database and either credentialsSecret or username/password. See https://github.com/signalfx/splunk-otel-db2#installation" $prefix) -}}
{{- end -}}
{{- range $i, $r := .Values.receivers -}}
{{- /* Check the element's kind before `get`, which otherwise reports
       "expected map[string]interface {}; got string" with no indication of where. */ -}}
{{- if not (kindIs "map" $r) -}}
{{- fail (printf "%s[%d] must be a map of receiver settings, got %s" $prefix $i (kindOf $r)) -}}
{{- end -}}
{{- range $field := list "name" "hostname" "database" -}}
{{- if not (get $r $field) -}}
{{- fail (printf "%s[%d].%s is required" $prefix $i $field) -}}
{{- end -}}
{{- end -}}
{{- /* `tls:` with no value parses as null, so normalise before reading through it. */ -}}
{{- $tls := $r.tls | default dict -}}
{{- if and (not (dig "insecure" false $tls)) (not (hasKey $r "port")) -}}
{{- fail (printf "%s[%d].port is required when TLS is enabled. Db2 requires ssl_svcename to listen on a different port from svcename, so the cleartext default (50000) would send a TLS handshake to the cleartext listener. Set the port your ssl_svcename uses, or set tls.insecure: true to connect without TLS." $prefix $i) -}}
{{- end -}}
{{- /* A CA is only consulted during a TLS handshake, so the pair is contradictory and one
       of the two settings is not what was meant. Silently honouring insecure would mount
       the Secret and still send the credentials in the clear. */ -}}
{{- if and (dig "caSecret" "" $tls) (dig "insecure" false $tls) -}}
{{- fail (printf "%s[%d].tls sets both caSecret and insecure: true. insecure disables TLS entirely, so the certificate authority would never be consulted and the credentials would travel unencrypted. Remove insecure to use the CA, or remove caSecret to connect without TLS." $prefix $i) -}}
{{- end -}}
{{- if and (not $r.credentialsSecret) (not $r.username) -}}
{{- fail (printf "%s[%d] needs credentials: set credentialsSecret to a pre-existing Secret holding DB2_USER and DB2_PASSWORD, or set username and password" $prefix $i) -}}
{{- end -}}
{{- end -}}
{{- end }}

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

{{/*
Per-receiver token identifying a CA mount: the receiver name, or a hash of it when the name
is too long to fit a DNS label once prefixed.

Derived from the receiver rather than the Secret: two receivers may share one authority,
which would collide, and a Secret name may contain dots, which a volume name may not.

Hashing rather than rejecting long names keeps the length constraint off receivers that never
mount a CA. The volume name, the mount path and the rendered ca_file all come from this one
token, so they cannot drift apart.
*/}}
{{- define "splunk-otel-db2.caToken" -}}
{{- if gt (len (printf "ca-%s" .name)) 63 -}}
{{- sha256sum .name | trunc 16 -}}
{{- else -}}
{{- .name -}}
{{- end -}}
{{- end -}}

{{/*
Directory a receiver's CA is mounted at. A sibling of the config mount, not a child of it:
nesting under /etc/splunk-otel-db2 works, but it leaves a directory inside what is otherwise
a read-only ConfigMap mount, and the two mounts have nothing to do with each other.
*/}}
{{- define "splunk-otel-db2.caMountPath" -}}
{{- printf "/etc/splunk-otel-db2-ca/%s" (include "splunk-otel-db2.caToken" .) -}}
{{- end -}}
