#!/usr/bin/env bash
#
# Pull the Db2 test image before Testcontainers needs it.
#
# The integration tests start a real Db2 container. That image is several GB and comes from
# icr.io, which is slow and occasionally throttles. Testcontainers applies its own deadline
# to image resolution, and when the pull exceeds it the failure surfaces as a
# ContainerFetchException during test initialization — which reads like a broken test rather
# than a registry problem.
#
# Pulling here keeps that concern out of the test run: the tests find the image locally and
# only pay for container startup. Transient registry failures are retried, and a genuine
# failure is reported as exactly that.
set -uo pipefail

readonly IMAGE="${1:?usage: pull-db2-image.sh <image-ref>}"
readonly ATTEMPTS="${PULL_ATTEMPTS:-3}"
readonly BACKOFF_SECONDS="${PULL_BACKOFF_SECONDS:-15}"

if docker image inspect "$IMAGE" >/dev/null 2>&1; then
  echo "Image already present locally: $IMAGE"
  exit 0
fi

for attempt in $(seq 1 "$ATTEMPTS"); do
  echo "Pulling $IMAGE (attempt $attempt/$ATTEMPTS)..."
  start=$SECONDS
  if docker pull --quiet "$IMAGE"; then
    echo "Pulled $IMAGE in $((SECONDS - start))s"
    docker image inspect --format '{{.Id}} {{.Size}} bytes' "$IMAGE"
    exit 0
  fi
  echo "Pull attempt $attempt failed after $((SECONDS - start))s"
  if [ "$attempt" -lt "$ATTEMPTS" ]; then
    echo "Retrying in ${BACKOFF_SECONDS}s..."
    sleep "$BACKOFF_SECONDS"
  fi
done

echo "::error::Failed to pull $IMAGE after $ATTEMPTS attempts. This is a registry or network" \
     "failure, not a test failure."
exit 1
