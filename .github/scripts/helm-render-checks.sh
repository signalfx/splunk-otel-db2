#!/usr/bin/env bash
#
# Render the chart the way it is actually consumed and assert the results.
#
# `helm lint` is not sufficient here. It reports a template `fail` as an INFO line and still
# exits 0, so every guard in this chart — missing credentials, missing endpoint, bad
# collectorTarget — passes lint whether or not it works. These checks use `helm template`,
# where a `fail` is a non-zero exit.
#
# The subchart cases matter because the OTLP endpoint is derived from the parent release
# name, which cannot be exercised standalone. The expected Service names below are the ones
# splunk-otel-collector actually renders: the gateway takes the release fullname and the
# agent appends -agent.
set -uo pipefail

readonly CHART_DIR="${CHART_DIR:-packaging/helm}"
readonly VALUES="${VALUES:-$CHART_DIR/ci/standalone-values.yaml}"
readonly RELEASE=testrel

# Helm 3 and Helm 4 word schema violations differently, and the chart supports both:
#   helm 3.12: "- receivers.0.port: Invalid type. Expected: integer, given: null"
#   helm 4.3:  "- at '/receivers/0/port': got null, want integer"
# Asserting either string verbatim fails on the other version, so match on the parts both
# carry: that it was the values schema, which field, and the expected type.
schema_rejected() {
  local out="$1" field="$2" want="$3"
  grep -Fq -- "don't meet the specifications of the schema" <<<"$out" || return 1
  grep -Fq -- "$field" <<<"$out" || return 1
  [ -z "$want" ] || grep -Fiq -- "$want" <<<"$out" || return 1
  return 0
}

failures=0

fail() {
  echo "  FAIL: $*" >&2
  failures=$((failures + 1))
}

pass() {
  echo "  ok: $*"
}

# Build a stub parent so the chart renders through Helm's real subchart machinery, including
# the global-only value propagation that a hand-written test cannot reproduce.
workdir=$(mktemp -d)
trap 'rm -rf "$workdir"' EXIT

readonly PARENT="$workdir/splunk-otel-collector"
mkdir -p "$PARENT/charts"
cat >"$PARENT/Chart.yaml" <<'EOF'
apiVersion: v2
name: splunk-otel-collector
version: 0.0.0
EOF
echo '{}' >"$PARENT/values.yaml"
cp -r "$CHART_DIR" "$PARENT/charts/splunk-otel-db2"
rm -f "$PARENT/charts/splunk-otel-db2/Chart.lock"

# Subchart values must be nested under the subchart name.
readonly SUB_VALUES="$workdir/sub-values.yaml"
{
  echo 'splunk-otel-db2:'
  sed 's/^/  /' "$VALUES"
} >"$SUB_VALUES"
# The derived endpoint is the thing under test, so drop the explicit one.
yq e -i 'del(.["splunk-otel-db2"].otlp.endpoint)' "$SUB_VALUES"

endpoint_of() {
  yq e -N 'select(.kind == "ConfigMap") | .data["config.yaml"]' - \
    | yq e -N '.exporters.otlp.endpoint'
}

echo "1. standalone renders with credentials and an explicit endpoint"
if out=$(helm template "$RELEASE" "$CHART_DIR" -f "$VALUES" 2>&1); then
  got=$(printf '%s' "$out" | endpoint_of)
  [ "$got" = "http://otel-collector:4317" ] \
    && pass "endpoint honoured verbatim: $got" \
    || fail "expected the explicit endpoint, got '$got'"
else
  fail "standalone render failed: $out"
fi

echo "2. standalone without an endpoint is rejected"
if out=$(helm template "$RELEASE" "$CHART_DIR" -f "$VALUES" --set otlp.endpoint= 2>&1); then
  fail "render succeeded; a standalone install has no derivable endpoint and must be rejected"
else
  grep -Fq -- 'otlp.endpoint is required' <<<"$out" \
    && pass "rejected with the actionable message" \
    || fail "rejected, but not for the endpoint; got: $out"
fi

echo "3. subchart defaults to the node-local agent Service"
if out=$(helm template "$RELEASE" "$PARENT" -f "$SUB_VALUES" 2>&1); then
  got=$(printf '%s' "$out" | endpoint_of)
  [ "$got" = "http://${RELEASE}-splunk-otel-collector-agent.default.svc.cluster.local:4317" ] \
    && pass "derived $got" \
    || fail "expected the agent Service, got '$got'"
else
  fail "subchart render failed: $out"
fi

echo "4. parent-supplied global.gateway.enabled selects the gateway Service"
if out=$(helm template "$RELEASE" "$PARENT" -f "$SUB_VALUES" \
           --set global.gateway.enabled=true 2>&1); then
  got=$(printf '%s' "$out" | endpoint_of)
  [ "$got" = "http://${RELEASE}-splunk-otel-collector.default.svc.cluster.local:4317" ] \
    && pass "derived $got" \
    || fail "expected the gateway Service, got '$got'"
else
  fail "subchart render with global.gateway.enabled failed: $out"
fi

echo "5. collectorTarget selects the gateway without parent cooperation"
if out=$(helm template "$RELEASE" "$PARENT" -f "$SUB_VALUES" \
           --set 'splunk-otel-db2.otlp.collectorTarget=gateway' 2>&1); then
  got=$(printf '%s' "$out" | endpoint_of)
  [ "$got" = "http://${RELEASE}-splunk-otel-collector.default.svc.cluster.local:4317" ] \
    && pass "derived $got" \
    || fail "expected the gateway Service, got '$got'"
else
  fail "subchart render with collectorTarget=gateway failed: $out"
fi

echo "6. an unknown collectorTarget is rejected"
if out=$(helm template "$RELEASE" "$PARENT" -f "$SUB_VALUES" \
           --set 'splunk-otel-db2.otlp.collectorTarget=nope' 2>&1); then
  fail "render succeeded with collectorTarget=nope; it must be rejected"
else
  schema_rejected "$out" "collectorTarget" "" \
    && pass "rejected by the schema enum, naming the field" \
    || fail "rejected, but not for collectorTarget; got: $out"
fi

echo "7. the derived name collapses when the release name contains the chart name"
# The collector's fullname helper does not append the chart-name suffix when the release
# name already contains it. These release names are the regression that an arbitrary name
# like "testrel" cannot catch, and "my-splunk-otel-collector" is the one in the collector
# chart's own documented install command.
for rel in my-splunk-otel-collector splunk-otel-collector splunk-otel-collector-prod; do
  if out=$(helm template "$rel" "$PARENT" -f "$SUB_VALUES" 2>&1); then
    got=$(printf '%s' "$out" | endpoint_of)
    [ "$got" = "http://${rel}-agent.default.svc.cluster.local:4317" ] \
      && pass "release '$rel' derived $got" \
      || fail "release '$rel': expected http://${rel}-agent.default.svc.cluster.local:4317, got '$got'"
  else
    fail "release '$rel' render failed: $out"
  fi
done

echo "8. the endpoint namespace follows the release, and collectorNamespace overrides it"
# The parent can relocate the collector Services with namespaceOverride, which a subchart
# cannot see. collectorNamespace is the escape hatch that avoids writing out a full endpoint.
if out=$(helm template "$RELEASE" "$PARENT" -f "$SUB_VALUES" --namespace observability 2>&1); then
  got=$(printf '%s' "$out" | endpoint_of)
  [ "$got" = "http://${RELEASE}-splunk-otel-collector-agent.observability.svc.cluster.local:4317" ] \
    && pass "follows the release namespace: $got" \
    || fail "expected the release namespace in the FQDN, got '$got'"
else
  fail "render in a non-default namespace failed: $out"
fi
if out=$(helm template "$RELEASE" "$PARENT" -f "$SUB_VALUES" --namespace observability \
           --set 'splunk-otel-db2.otlp.collectorNamespace=collectors' 2>&1); then
  got=$(printf '%s' "$out" | endpoint_of)
  [ "$got" = "http://${RELEASE}-splunk-otel-collector-agent.collectors.svc.cluster.local:4317" ] \
    && pass "collectorNamespace override respected: $got" \
    || fail "expected the overridden namespace, got '$got'"
else
  fail "render with collectorNamespace failed: $out"
fi

echo "9. a minimal TLS entry renders, and defaults what it omits"
# Helm replaces a list wholesale, so a user entry inherits nothing from the default entry.
# Every field not listed here must therefore default, or enabling the chart aborts the
# parent chart's entire render — which is how four reviewers hit this.
minimal="$workdir/minimal.yaml"
cat >"$minimal" <<'EOF'
splunk-otel-db2:
  receivers:
    - name: default
      hostname: db2.example.com
      database: PROD
      credentialsSecret: my-db2-creds
      port: 50001
EOF
if out=$(helm template "$RELEASE" "$PARENT" -f "$minimal" 2>&1); then
  cfg=$(printf '%s' "$out" | yq e -N 'select(.kind == "ConfigMap") | .data["config.yaml"]' -)
  got_tls=$(printf '%s' "$cfg" | yq e -N '.receivers["db2/default"].tls.insecure' -)
  got_port=$(printf '%s' "$cfg" | yq e -N '.receivers["db2/default"].hostport' -)
  got_int=$(printf '%s' "$cfg" | yq e -N '.receivers["db2/default"].collection_interval' -)
  [ "$got_tls" = "false" ] \
    && pass "tls.insecure defaults to false — credentials are not sent in cleartext" \
    || fail "tls.insecure should default to false, got '$got_tls'"
  [ "$got_port" = "50001" ] && pass "the explicit TLS port is used" || fail "port, got '$got_port'"
  [ "$got_int" = "30s" ] && pass "collectionInterval defaults to 30s" || fail "interval default, got '$got_int'"
else
  fail "a minimal receiver entry must render, got: $out"
fi

echo "10. enabling the chart without configuring credentials fails, naming the values path"
# The shipped entry is a placeholder to edit, so enabling alone leaves credentials unset.
# That must fail - a receiver with no credentials cannot connect - and the message has to
# name what the reader has to set, in the path they would search for.
credsonly="$workdir/creds-only.yaml"
printf 'splunk-otel-db2:\n  receivers:\n    - name: default\n      hostname: h\n      database: d\n      port: 50001\n' >"$credsonly"
if out=$(helm template "$RELEASE" "$PARENT" -f "$credsonly" 2>&1); then
  fail "rendering with unset credentials succeeded; it must be rejected"
else
  grep -Fq -- 'db2.receivers[0] needs credentials' <<<"$out" \
    && pass "rejected, naming db2.receivers[0] and what to set" \
    || fail "rejected, but without naming db2.receivers; got: $out"
fi

echo "10b. an explicitly empty receivers list fails too"
# via a values file: `--set x=[]` assigns the literal string "[]", not an empty list.
empty="$workdir/empty.yaml"
printf 'splunk-otel-db2:\n  receivers: []\n' >"$empty"
if out=$(helm template "$RELEASE" "$PARENT" -f "$empty" 2>&1); then
  fail "an empty receivers list rendered; it must be rejected"
else
  grep -Fq -- 'db2.receivers is empty' <<<"$out" \
    && pass "rejected with the emptiness message" \
    || fail "rejected, but not for emptiness; got: $out"
fi

echo "10c. a non-list receivers value is rejected clearly"
if out=$(helm template "$RELEASE" "$PARENT" --set 'splunk-otel-db2.receivers=oops' 2>&1); then
  fail "a string receivers value rendered; it must be rejected"
else
  schema_rejected "$out" "receivers" "" \
    && pass "rejected by the schema, naming the field" \
    || fail "rejected, but with an unhelpful error; got: $out"
fi

echo "10d. a non-map element inside the list is rejected clearly"
if out=$(helm template "$RELEASE" "$PARENT" --set 'splunk-otel-db2.receivers[0]=oops' 2>&1); then
  fail "a string list element rendered; it must be rejected"
else
  schema_rejected "$out" "receivers" "" \
    && pass "rejected by the schema, naming the element" \
    || fail "rejected, but with an unhelpful error; got: $out"
fi

echo "10e. TLS on without an explicit port is rejected, naming ssl_svcename"
# TLS defaults on, but the port default (50000) is Db2's cleartext listener: ssl_svcename must
# differ from svcename, so guessing the port would send a TLS handshake to the wrong one.
tlsnoport="$workdir/tls-noport.yaml"
cat >"$tlsnoport" <<'EOF'
splunk-otel-db2:
  receivers:
    - name: default
      hostname: db2.example.com
      database: PROD
      credentialsSecret: s
EOF
if out=$(helm template "$RELEASE" "$PARENT" -f "$tlsnoport" 2>&1); then
  fail "TLS with a guessed port rendered; the port must be required"
else
  grep -Fq -- 'ssl_svcename' <<<"$out" \
    && pass "rejected, explaining why the cleartext default cannot be reused" \
    || fail "rejected, but without explaining the port; got: $out"
fi

echo "10f. an explicitly null tls block does not crash, and means TLS on"
nulltls="$workdir/null-tls.yaml"
cat >"$nulltls" <<'EOF'
splunk-otel-db2:
  receivers:
    - name: default
      hostname: db2.example.com
      database: PROD
      credentialsSecret: s
      port: 50001
      tls:
EOF
if out=$(helm template "$RELEASE" "$PARENT" -f "$nulltls" 2>&1); then
  cfg=$(printf '%s' "$out" | yq e -N 'select(.kind == "ConfigMap") | .data["config.yaml"]' -)
  got=$(printf '%s' "$cfg" | yq e -N '.receivers["db2/default"].tls.insecure' -)
  [ "$got" = "false" ] \
    && pass "null tls renders as insecure: false rather than erroring" \
    || fail "expected insecure false from a null tls block, got '$got'"
else
  fail "a null tls block must not crash rendering, got: $out"
fi

echo "10g. insecure: true keeps the cleartext port default"
insec="$workdir/insecure.yaml"
cat >"$insec" <<'EOF'
splunk-otel-db2:
  receivers:
    - name: default
      hostname: db2.example.com
      database: PROD
      credentialsSecret: s
      tls:
        insecure: true
EOF
if out=$(helm template "$RELEASE" "$PARENT" -f "$insec" 2>&1); then
  cfg=$(printf '%s' "$out" | yq e -N 'select(.kind == "ConfigMap") | .data["config.yaml"]' -)
  got=$(printf '%s' "$cfg" | yq e -N '.receivers["db2/default"].hostport' -)
  [ "$got" = "50000" ] \
    && pass "cleartext path still defaults the port to 50000" \
    || fail "expected the 50000 default on the cleartext path, got '$got'"
else
  fail "the cleartext path must not require a port, got: $out"
fi

echo "10h. the values schema rejects wrong types before any template runs"
# These three all passed template-level checks previously: hasKey is true for a null value,
# a non-empty string is truthy so "false" took the TLS-off branch, and `tls: true` reached dig.
schema_case() {
  f="$workdir/schema-$1.yaml"
  { echo 'splunk-otel-db2:'; printf '%s\n' "$2" | sed 's/^/  /'; } >"$f"
  if out=$(helm template "$RELEASE" "$PARENT" -f "$f" 2>&1); then
    fail "$1 rendered; the schema must reject it"
  else
    schema_rejected "$out" "$3" "$4" \
      && pass "$1 rejected on $3 (want $4)" \
      || fail "$1 rejected, but not by the schema on $3; got: $out"
  fi
}
schema_case null-port \
  'receivers:
  - name: default
    hostname: h
    database: d
    credentialsSecret: s
    port: null' \
  port integer
schema_case string-insecure \
  'receivers:
  - name: default
    hostname: h
    database: d
    credentialsSecret: s
    tls:
      insecure: "false"' \
  insecure boolean
schema_case bool-tls \
  'receivers:
  - name: default
    hostname: h
    database: d
    credentialsSecret: s
    port: 50001
    tls: true' \
  tls object

echo "10g. a mistyped receiver key is rejected rather than silently defaulted"
# The chart reads a fixed set of keys, so anything else is a typo. Without
# additionalProperties these validated cleanly and the receiver ran on the default, which
# looks identical to a working install until you notice the interval never changed.
for typo in collectionIntervl topQueryCollecton; do
  if out=$(helm template "$RELEASE" "$PARENT" \
             --set 'splunk-otel-db2.receivers[0].name=default' \
             --set 'splunk-otel-db2.receivers[0].hostname=h' \
             --set 'splunk-otel-db2.receivers[0].database=d' \
             --set 'splunk-otel-db2.receivers[0].credentialsSecret=s' \
             --set 'splunk-otel-db2.receivers[0].port=50001' \
             --set "splunk-otel-db2.receivers[0].$typo=1s" 2>&1); then
    fail "$typo was accepted; a key the chart never reads must be rejected"
  else
    grep -Fiq -- "$typo" <<<"$out" \
      && pass "$typo rejected, naming the key" \
      || fail "$typo rejected, but without naming it; got: $out"
  fi
done

echo "10h. an optional block written with no value still renders"
# `events:` with nothing after it parses as null, and dig cannot traverse a nil
# intermediate - it aborted the whole render, which in a subchart takes the parent's
# install with it. Both nesting levels are covered because both can be left empty.
cat >"$workdir/nullblocks.yaml" <<'EOF'
splunk-otel-db2:
  receivers:
    - name: default
      hostname: db2.example.com
      database: PROD
      credentialsSecret: s
      port: 50001
      events:
        dbServerTopQuery:
        dbServerQuerySample:
      topQueryCollection:
      querySampleCollection:
EOF
if out=$(helm template "$RELEASE" "$PARENT" -f "$workdir/nullblocks.yaml" 2>&1); then
  cfg=$(printf '%s' "$out" | yq e -N 'select(.kind == "ConfigMap") | .data["config.yaml"]' -)
  got=$(printf '%s' "$cfg" | yq e -N '.receivers["db2/default"] | [.top_query_collection.top_query_count, .query_sample_collection.max_rows_per_query, .events["db.server.top_query"].enabled] | join(",")' -)
  [ "$got" = "250,100,false" ] \
    && pass "null blocks fall back to the documented defaults: $got" \
    || fail "expected 250,100,false from null blocks, got '$got'"
else
  fail "null optional blocks must render, not abort; got: $out"
fi

echo "11. a receiver missing a required field names that field"
if out=$(helm template "$RELEASE" "$PARENT" \
           --set 'splunk-otel-db2.receivers[0].name=default' \
           --set 'splunk-otel-db2.receivers[0].database=PROD' \
           --set 'splunk-otel-db2.receivers[0].credentialsSecret=s' 2>&1); then
  fail "rendering without hostname succeeded; it must be rejected"
else
  schema_rejected "$out" "hostname" "" \
    && pass "names the exact missing field" \
    || fail "rejected, but without naming the field; got: $out"
fi

echo "11b. a CA Secret is mounted and the rendered config points at the mount"
# The config path and the mount path are rendered from the same receiver name rather than
# configured separately, so they cannot disagree - which is the failure this would invite.
ca="$workdir/ca.yaml"
dep='select(.kind == "Deployment") | .spec.template.spec'
cat >"$ca" <<'EOF'
splunk-otel-db2:
  receivers:
    - name: default
      hostname: db2.example.com
      database: PROD
      credentialsSecret: s
      port: 50001
      tls:
        caSecret: my-db2-ca
EOF
if out=$(helm template "$RELEASE" "$PARENT" -f "$ca" 2>&1); then
  cfg=$(printf '%s' "$out" | yq e -N 'select(.kind == "ConfigMap") | .data["config.yaml"]' -)
  loc=$(printf '%s' "$cfg" | yq e -N '.receivers["db2/default"].tls.ca_file' -)
  mnt=$(printf '%s' "$out" | yq e -N "$dep"' | .containers[0].volumeMounts[] | select(.name == "ca-default") | .mountPath' -)
  sec=$(printf '%s' "$out" | yq e -N "$dep"' | .volumes[] | select(.name == "ca-default") | .secret.secretName' -)
  [ "$sec" = "my-db2-ca" ] && pass "the named Secret is mounted" || fail "secret mount, got '$sec'"
  [ "$loc" = "$mnt/ca.crt" ] \
    && pass "ca_file sits under the mount: $loc" \
    || fail "ca_file '$loc' is not under the mount '$mnt'"
  # A sibling of the config mount, not a child. Nesting does work - the runtime creates the
  # mount point before the container starts, verified in kind under this chart's non-root,
  # read-only-rootfs security context - but it leaves a directory inside what is otherwise a
  # read-only ConfigMap mount, and the two mounts are unrelated.
  cfgmnt=$(printf '%s' "$out" | yq e -N "$dep"' | .containers[0].volumeMounts[] | select(.name == "config") | .mountPath' -)
  case "$mnt" in
    "$cfgmnt"/*) fail "the CA mount '$mnt' nests inside the config mount '$cfgmnt'" ;;
    *) pass "the CA mount is a sibling of the config mount, not nested" ;;
  esac
else
  fail "a receiver with tls.caSecret must render, got: $out"
fi

echo "11c. only caSecretKey is projected, under a fixed name"
# Mounting the whole Secret would hand every other key in it to the container, and would
# make the file name depend on the key, which the config would then have to track.
printf '%s\n' "        caSecretKey: corp-root.pem" >>"$ca"
if out=$(helm template "$RELEASE" "$PARENT" -f "$ca" 2>&1); then
  items=$(printf '%s' "$out" | yq e -N "$dep"' | .volumes[] | select(.name == "ca-default") | .secret.items | map(.key + "->" + .path) | join(",")' -)
  loc=$(printf '%s' "$out" | yq e -N 'select(.kind == "ConfigMap") | .data["config.yaml"]' - \
        | yq e -N '.receivers["db2/default"].tls.ca_file' -)
  [ "$items" = "corp-root.pem->ca.crt" ] \
    && pass "just that key, projected to ca.crt" \
    || fail "expected only corp-root.pem->ca.crt, got '$items'"
  [ "${loc##*/}" = "ca.crt" ] \
    && pass "the config path is unaffected by the key name" \
    || fail "expected the config to end in /ca.crt, got '$loc'"
else
  fail "caSecretKey must render, got: $out"
fi

echo "11d. receivers sharing one CA Secret get distinct, legal volume names"
# Naming the volume after the Secret produced duplicates here, and a Secret name containing
# dots is legal for a Secret but not for a volume. Both are why this is keyed by receiver.
cat >"$workdir/ca-shared.yaml" <<'EOF'
splunk-otel-db2:
  receivers:
    - name: alpha
      hostname: a.example.com
      database: A
      credentialsSecret: s
      port: 50001
      tls:
        caSecret: pki.example.com
    - name: beta
      hostname: b.example.com
      database: B
      credentialsSecret: s
      port: 50001
      tls:
        caSecret: pki.example.com
EOF
if out=$(helm template "$RELEASE" "$PARENT" -f "$workdir/ca-shared.yaml" 2>&1); then
  names=$(printf '%s' "$out" | yq e -N "$dep"' | .volumes[] | select(.name | test("^ca-")) | .name' - | sort | paste -sd, -)
  [ "$names" = "ca-alpha,ca-beta" ] \
    && pass "one volume per receiver: $names" \
    || fail "expected ca-alpha,ca-beta, got '$names'"
  printf '%s' "$names" | grep -q '\.' \
    && fail "a volume name contains a dot, which Kubernetes rejects for a DNS label" \
    || pass "no Secret name leaked into a volume name"
else
  fail "two receivers sharing a CA Secret must render, got: $out"
fi

echo "11e. a long receiver name still yields a legal volume name"
# The first attempt capped every receiver name at 60 characters so ca-<name> would fit a DNS
# label. That penalised configurations that never mount a CA, so the name is hashed instead.
long=$(printf 'a%.0s' $(seq 70))
if out=$(helm template "$RELEASE" "$PARENT" \
           --set "splunk-otel-db2.receivers[0].name=$long" \
           --set 'splunk-otel-db2.receivers[0].hostname=h' \
           --set 'splunk-otel-db2.receivers[0].database=d' \
           --set 'splunk-otel-db2.receivers[0].credentialsSecret=s' \
           --set 'splunk-otel-db2.receivers[0].port=50001' \
           --set 'splunk-otel-db2.receivers[0].tls.caSecret=my-ca' 2>&1); then
  vol=$(printf '%s' "$out" | yq e -N "$dep"' | .volumes[] | select(.name | test("^ca-")) | .name' -)
  [ -n "$vol" ] && [ "${#vol}" -le 63 ] \
    && pass "volume name is ${#vol} characters: $vol" \
    || fail "expected a volume name within 63 characters, got '$vol' (${#vol})"
  loc=$(printf '%s' "$out" | yq e -N 'select(.kind == "ConfigMap") | .data["config.yaml"]' - \
        | yq e -N ".receivers[\"db2/$long\"].tls.ca_file" -)
  [ "$loc" = "/etc/splunk-otel-db2-ca/${vol#ca-}/ca.crt" ] \
    && pass "the config path follows the same derived token: $loc" \
    || fail "ca_file '$loc' does not match the volume '$vol'"
else
  fail "a 70-character receiver name must render, got: $out"
fi

echo "11f. a Secret name the Kubernetes API would reject is caught by the schema"
# The first pattern allowed a..b, a.-b and over-long labels: Helm rendered them and the
# failure only appeared later as a Secret reference that could not be resolved.
label64=$(printf 'b%.0s' $(seq 64))
for bad in "a..b" "a.-b" "$label64.example.com" "-abc"; do
  if out=$(helm template "$RELEASE" "$PARENT" \
             --set 'splunk-otel-db2.receivers[0].name=default' \
             --set 'splunk-otel-db2.receivers[0].hostname=h' \
             --set 'splunk-otel-db2.receivers[0].database=d' \
             --set 'splunk-otel-db2.receivers[0].credentialsSecret=s' \
             --set 'splunk-otel-db2.receivers[0].port=50001' \
             --set "splunk-otel-db2.receivers[0].tls.caSecret=$bad" 2>&1); then
    fail "caSecret '$bad' rendered; Kubernetes would reject that Secret name"
  else
    grep -Fq -- "caSecret" <<<"$out" \
      && pass "rejected '$bad', naming caSecret" \
      || fail "'$bad' rejected, but not for caSecret; got: $out"
  fi
done
for good in my-db2-ca pki.example.com; do
  helm template "$RELEASE" "$PARENT" \
    --set 'splunk-otel-db2.receivers[0].name=default' \
    --set 'splunk-otel-db2.receivers[0].hostname=h' \
    --set 'splunk-otel-db2.receivers[0].database=d' \
    --set 'splunk-otel-db2.receivers[0].credentialsSecret=s' \
    --set 'splunk-otel-db2.receivers[0].port=50001' \
    --set "splunk-otel-db2.receivers[0].tls.caSecret=$good" >/dev/null 2>&1 \
    && pass "still accepts the legal name '$good'" \
    || fail "legal Secret name '$good' was rejected"
done

echo "11g. a CA together with insecure is rejected"
# Honouring insecure here would mount the CA, never consult it, and send the credentials in
# the clear - so the contradiction is reported rather than resolved silently.
if out=$(helm template "$RELEASE" "$PARENT" -f "$ca" \
           --set 'splunk-otel-db2.receivers[0].tls.insecure=true' 2>&1); then
  fail "caSecret with insecure: true rendered; it must be rejected"
else
  grep -Fq -- "sets both caSecret and insecure" <<<"$out" \
    && pass "rejected, explaining which of the two to drop" \
    || fail "rejected, but not for the contradiction; got: $out"
fi

echo "11h. no CA Secret means no mount and no ca_file"
if out=$(helm template "$RELEASE" "$PARENT" -f "$minimal" 2>&1); then
  n=$(grep -c "ca_file" <<<"$out" || true)
  v=$(grep -c "name: ca-" <<<"$out" || true)
  [ "$n" = "0" ] && [ "$v" = "0" ] \
    && pass "neither is emitted, so the driver keeps the JVM truststore" \
    || fail "ca_file=$n ca volumes=$v, expected none"
else
  fail "the minimal entry must still render, got: $out"
fi

echo "11k. the duration pattern matches exactly what the receiver's parser accepts"
# The schema exists so a bad duration is a values error rather than a pod that exits once
# scheduled. Being stricter than the parser is its own bug: it fails upgrades that worked.
# The parser trims surrounding whitespace and accepts leading zeros, so these must too.
duration_case() {
  local value="$1" want="$2" out
  if out=$(helm template "$RELEASE" "$CHART_DIR" -f "$VALUES" \
             --set-string "receivers[0].querySampleCollection.collectionInterval=$value" 2>&1); then
    [ "$want" = accept ] \
      && pass "duration '$value' accepted" \
      || fail "duration '$value' was accepted; the parser rejects it"
  else
    if grep -Fq -- "collectionInterval" <<<"$out"; then
      [ "$want" = reject ] \
        && pass "duration '$value' rejected by the schema" \
        || fail "duration '$value' was rejected; the parser accepts it"
    else
      fail "duration '$value' failed for an unrelated reason: $out"
    fi
  fi
}
# Accepted by Db2ReceiverConfig.parseDuration, so accepted here.
for v in "30s" "1s" "01s" "05s" "010s" "5m" "1h" " 30s " "  05s"; do duration_case "$v" accept; done
# Rejected by the parser, so rejected here.
for v in "0s" "00s" " 0s " "500ms" " 500ms " "1x" "s" "abc"; do duration_case "$v" reject; done

echo "11i. the chart declares its own enabled: false for a parent condition"
# A parent gates this subchart on `condition: <alias>.enabled`, resolved against the
# coalesced values. Without this key here, a parent whose own key is missing enables the
# subchart - which is what `helm upgrade --reuse-values` does from a release predating it.
got=$(yq e -N '.enabled' "$CHART_DIR/values.yaml")
[ "$got" = "false" ] \
  && pass "values.yaml ships enabled: false" \
  || fail "values.yaml .enabled is '$got', expected false"
# Standalone installs must be unaffected by it.
if out=$(helm template "$RELEASE" "$CHART_DIR" -f "$VALUES" 2>&1); then
  n=$(printf '%s' "$out" | grep -c "^kind:" || true)
  [ "$n" -gt 0 ] \
    && pass "a standalone install still renders ($n objects)" \
    || fail "a standalone install rendered nothing"
else
  fail "a standalone install must render, got: $out"
fi

echo "11j. the parent condition itself resolves, with and without the parent's key"
# The check above only proves the key is present. This renders through a parent that
# actually declares `alias: db2` and `condition: db2.enabled`, which is what the key exists
# for - the shared $PARENT above has no dependencies block, so it renders the subchart
# unconditionally and cannot exercise this.
COND_PARENT="$workdir/cond-parent"
mkdir -p "$COND_PARENT/charts"
chart_version=$(yq e -N '.version' "$CHART_DIR/Chart.yaml")
cat >"$COND_PARENT/Chart.yaml" <<EOF
apiVersion: v2
name: splunk-otel-collector
version: 0.0.0
dependencies:
  - name: splunk-otel-db2
    version: $chart_version
    alias: db2
    condition: db2.enabled
EOF
echo '{}' >"$COND_PARENT/values.yaml"
cp -r "$CHART_DIR" "$COND_PARENT/charts/splunk-otel-db2"

cond_objects() {
  # Renders the conditioned parent and reports how many objects came out, or ERROR.
  local out
  if out=$(helm template "$RELEASE" "$COND_PARENT" "$@" 2>&1); then
    printf '%s' "$out" | grep -c "^kind:" || true
  else
    echo "ERROR: $out"
  fi
}

n=$(cond_objects)
[ "$n" = "0" ] \
  && pass "no parent key: the condition resolves false and nothing renders" \
  || fail "no parent key should render nothing, got: $n"

cat >"$workdir/cond-on.yaml" <<'EOF'
db2:
  enabled: true
  receivers:
    - name: default
      hostname: db2.example.com
      database: PROD
      port: 50001
      credentialsSecret: creds
  otlp:
    endpoint: http://collector:4317
EOF
n=$(cond_objects -f "$workdir/cond-on.yaml")
[ "$n" -gt 0 ] 2>/dev/null \
  && pass "db2.enabled: true renders the subchart ($n objects)" \
  || fail "db2.enabled: true should render, got: $n"

n=$(cond_objects --set db2.enabled=false)
[ "$n" = "0" ] \
  && pass "db2.enabled: false renders nothing" \
  || fail "db2.enabled: false should render nothing, got: $n"

# Non-vacuous: drop the key from the subchart copy and the missing-key case must change.
yq e -i 'del(.enabled)' "$COND_PARENT/charts/splunk-otel-db2/values.yaml"
n=$(cond_objects)
[ "$n" = "0" ] \
  && fail "with the subchart's key dropped, a missing parent key still rendered nothing - the assertion above proves nothing" \
  || pass "control: dropping the key does enable the subchart, so the default is what disables it"
yq e -i '.enabled = false' "$COND_PARENT/charts/splunk-otel-db2/values.yaml"

echo "12. an explicit endpoint still wins as a subchart"
if out=$(helm template "$RELEASE" "$PARENT" -f "$SUB_VALUES" \
           --set 'splunk-otel-db2.otlp.endpoint=http://explicit:4317' 2>&1); then
  got=$(printf '%s' "$out" | endpoint_of)
  [ "$got" = "http://explicit:4317" ] \
    && pass "override respected: $got" \
    || fail "expected the explicit endpoint to win, got '$got'"
else
  fail "subchart render with an explicit endpoint failed: $out"
fi

echo "13. serviceInstanceId renders service_instance_id, and is absent when unset"
sid_of() {
  yq e -N 'select(.kind == "ConfigMap") | .data["config.yaml"]' - \
    | yq e -N '.receivers.["db2/default"].service_instance_id'
}
if out=$(helm template "$RELEASE" "$CHART_DIR" -f "$VALUES" \
           --set 'receivers[0].serviceInstanceId=prod-db2-01' 2>&1); then
  got=$(printf '%s' "$out" | sid_of)
  [ "$got" = "prod-db2-01" ] \
    && pass "override rendered: $got" \
    || fail "expected the override to render, got '$got'"
else
  fail "render with serviceInstanceId failed: $out"
fi
# Non-vacuous: without the key the field must be absent entirely, not an empty string.
# An empty service_instance_id would override the receiver's composed identity with
# nothing, which is worse than not setting it.
if out=$(helm template "$RELEASE" "$CHART_DIR" -f "$VALUES" 2>&1); then
  got=$(printf '%s' "$out" | sid_of)
  [ "$got" = "null" ] \
    && pass "absent when unset, so the receiver composes its own identity" \
    || fail "expected no service_instance_id key when unset, got '$got'"
else
  fail "render without serviceInstanceId failed: $out"
fi
# An empty string is a values error, not a silently empty override.
out=$(helm template "$RELEASE" "$CHART_DIR" -f "$VALUES" \
        --set 'receivers[0].serviceInstanceId=' 2>&1)
schema_rejected "$out" "serviceInstanceId" "" \
  && pass "an empty serviceInstanceId is rejected by the schema" \
  || fail "an empty serviceInstanceId should be a schema error; got: $out"

if [ "$failures" -ne 0 ]; then
  echo "::error::$failures helm render check(s) failed"
  exit 1
fi
echo "All helm render checks passed."
