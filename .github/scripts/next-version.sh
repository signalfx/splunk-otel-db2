#!/usr/bin/env bash
#
# Compute the version to publish for the chart and the image.
#
# The patch number is derived, not stored. Chart.yaml's version acts as a floor that pins
# only major.minor; this script reads the already-published versions from the gh-pages
# index.yaml and returns the next patch in that series. Deriving it means main can publish
# on every merge without a bot committing a version bump back to main — which would retrigger
# the release workflow and loop.
#
# Raising major or minor is a deliberate human act: edit the floor in Chart.yaml. Lowering the
# floor below what is already published is rejected, because chart-releaser refuses to
# overwrite an existing release and the failure it emits does not explain why.
#
# Prints "<major>.<minor>.<patch>" to stdout. When GITHUB_OUTPUT is set, also writes
# version=<value> for downstream jobs to consume.
set -uo pipefail

readonly CHART_FILE="${CHART_FILE:-packaging/helm/Chart.yaml}"
readonly CHART_NAME="${CHART_NAME:-splunk-otel-db2}"
# Owner and repo are overridable so the whole publish path can be rehearsed against a
# scratch public repository before it runs against the real one.
readonly CHART_REPO_OWNER="${CHART_REPO_OWNER:-signalfx}"
readonly CHART_REPO_NAME="${CHART_REPO_NAME:-splunk-otel-db2}"
readonly INDEX_URL="${INDEX_URL:-https://${CHART_REPO_OWNER}.github.io/${CHART_REPO_NAME}/index.yaml}"

die() {
  echo "::error::$*" >&2
  exit 1
}

command -v yq >/dev/null 2>&1 || die "yq is required but not installed"

[ -f "$CHART_FILE" ] || die "chart file not found: $CHART_FILE"

floor=$(yq e '.version' "$CHART_FILE")
[ -n "$floor" ] && [ "$floor" != "null" ] || die "could not read .version from $CHART_FILE"

if ! [[ "$floor" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
  die "chart version floor '$floor' is not a bare major.minor.patch; prerelease and build
metadata are not supported because the patch number is derived"
fi
readonly SERIES="${BASH_REMATCH[1]}.${BASH_REMATCH[2]}"
readonly FLOOR_PATCH="${BASH_REMATCH[3]}"

# A missing index is the expected state before the first release, so it must not be fatal.
# Distinguish that from a transport failure: publishing 0.1.0 over a transient 500 would
# collide with whatever is already released.
index=$(mktemp)
trap 'rm -f "$index"' EXIT
curl_out=$(curl -sS --max-time 30 -o "$index" -w '%{http_code} %{redirect_url}' "$INDEX_URL" 2>/dev/null)
curl_status=$?
http_code=${curl_out%% *}
redirect=${curl_out#* }

if [ "$curl_status" -ne 0 ]; then
  die "could not reach $INDEX_URL (curl exit $curl_status); refusing to guess a version"
fi

case "$http_code" in
  200)
    # A parse failure must be fatal. Treating malformed YAML as "nothing published" would
    # select the floor and overwrite a release that already exists.
    if ! parsed=$(yq e ".entries[\"${CHART_NAME}\"][].version // \"\"" "$index" 2>&1); then
      die "could not parse $INDEX_URL as a Helm repository index: $parsed"
    fi
    published=$(printf '%s\n' "$parsed" | grep -E '^[0-9]+\.[0-9]+\.[0-9]+$' || true)
    ;;
  404)
    echo "No index at $INDEX_URL yet; treating $floor as the first release" >&2
    published=""
    ;;
  30[1278])
    # GitHub serves Pages for a private or internal repository from a generated
    # *.pages.github.io host behind authentication, and redirects the canonical
    # <owner>.github.io/<repo> path there. Following the redirect does not help: the
    # destination requires a login, so no anonymous consumer can read the index either.
    die "$INDEX_URL redirected to ${redirect:-an unknown location}.
A redirect to a *.pages.github.io host means the repository is private or internal, so the
chart index is not anonymously reachable and neither Helm nor a dependent chart can fetch
it. The repository must be public for the canonical URL to serve. Refusing to guess a
version."
    ;;
  *)
    die "unexpected HTTP $http_code from $INDEX_URL; refusing to guess a version"
    ;;
esac

latest_in_series=$(printf '%s\n' "$published" \
  | grep -E "^${SERIES}\.[0-9]+$" \
  | sort -t. -k3,3n \
  | tail -1)

if [ -z "$latest_in_series" ]; then
  version="$floor"
else
  latest_patch="${latest_in_series##*.}"
  if [ "$FLOOR_PATCH" -gt "$latest_patch" ]; then
    die "chart version floor $floor is ahead of the published $latest_in_series; set the floor
to ${SERIES}.0 and let the patch derive, or bump minor instead"
  fi
  version="${SERIES}.$((latest_patch + 1))"
fi

# Guard against republishing an existing version, which chart-releaser rejects opaquely.
# Not a `... | grep -q` pipeline: grep -q exits on first match and SIGPIPEs the producer,
# which under pipefail reports failure even on a match.
if grep -Fxq -- "$version" <<<"$published"; then
  die "version $version is already published; nothing to release"
fi

echo "Floor $floor -> publishing $version" >&2
echo "$version"

if [ -n "${GITHUB_OUTPUT:-}" ]; then
  echo "version=$version" >>"$GITHUB_OUTPUT"
fi
