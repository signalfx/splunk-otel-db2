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
  printf '%s' "$out" | grep -q 'otlp.endpoint is required' \
    && pass "rejected with the actionable message" \
    || fail "rejected, but not for the endpoint; got: $out"
fi

echo "3. subchart defaults to the node-local agent Service"
if out=$(helm template "$RELEASE" "$PARENT" -f "$SUB_VALUES" 2>&1); then
  got=$(printf '%s' "$out" | endpoint_of)
  [ "$got" = "http://${RELEASE}-splunk-otel-collector-agent:4317" ] \
    && pass "derived $got" \
    || fail "expected the agent Service, got '$got'"
else
  fail "subchart render failed: $out"
fi

echo "4. parent-supplied global.gateway.enabled selects the gateway Service"
if out=$(helm template "$RELEASE" "$PARENT" -f "$SUB_VALUES" \
           --set global.gateway.enabled=true 2>&1); then
  got=$(printf '%s' "$out" | endpoint_of)
  [ "$got" = "http://${RELEASE}-splunk-otel-collector:4317" ] \
    && pass "derived $got" \
    || fail "expected the gateway Service, got '$got'"
else
  fail "subchart render with global.gateway.enabled failed: $out"
fi

echo "5. collectorTarget selects the gateway without parent cooperation"
if out=$(helm template "$RELEASE" "$PARENT" -f "$SUB_VALUES" \
           --set 'splunk-otel-db2.otlp.collectorTarget=gateway' 2>&1); then
  got=$(printf '%s' "$out" | endpoint_of)
  [ "$got" = "http://${RELEASE}-splunk-otel-collector:4317" ] \
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
  printf '%s' "$out" | grep -q 'collectorTarget must be' \
    && pass "rejected with the actionable message" \
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
    [ "$got" = "http://${rel}-agent:4317" ] \
      && pass "release '$rel' derived $got" \
      || fail "release '$rel': expected http://${rel}-agent:4317, got '$got'"
  else
    fail "release '$rel' render failed: $out"
  fi
done

echo "8. an explicit endpoint still wins as a subchart"
if out=$(helm template "$RELEASE" "$PARENT" -f "$SUB_VALUES" \
           --set 'splunk-otel-db2.otlp.endpoint=http://explicit:4317' 2>&1); then
  got=$(printf '%s' "$out" | endpoint_of)
  [ "$got" = "http://explicit:4317" ] \
    && pass "override respected: $got" \
    || fail "expected the explicit endpoint to win, got '$got'"
else
  fail "subchart render with an explicit endpoint failed: $out"
fi

if [ "$failures" -ne 0 ]; then
  echo "::error::$failures helm render check(s) failed"
  exit 1
fi
echo "All helm render checks passed."
