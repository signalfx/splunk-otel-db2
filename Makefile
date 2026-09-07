.PHONY: build test integration-test run jar clean docker-build docker-push helm-lint help

IMAGE_REPO ?= ghcr.io/signalfx/splunk-otel-db2
IMAGE_TAG  ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")

build:
	./gradlew clean build

test:
	./gradlew test

integration-test:
	./gradlew integrationTest

run:
	@test -n "$(CONFIG)" || (echo "Usage: make run CONFIG=<path-to-config.yaml>"; exit 1)
	./gradlew run --args="$(CONFIG)"

jar:
	./gradlew jar

clean:
	./gradlew clean

docker-build:
	docker build -f packaging/docker/Dockerfile -t $(IMAGE_REPO):$(IMAGE_TAG) .

docker-push:
	docker push $(IMAGE_REPO):$(IMAGE_TAG)

helm-lint:
	helm lint packaging/helm

help:
	@echo "Available targets:"
	@echo "  build             - Clean and build (unit tests included)"
	@echo "  test              - Run unit tests"
	@echo "  integration-test  - Run integration tests (requires Docker)"
	@echo "  run               - Run the receiver locally (CONFIG=<path> required)"
	@echo "  jar               - Build fat jar"
	@echo "  clean             - Clean build outputs"
	@echo "  docker-build      - Build Docker image (IMAGE_REPO, IMAGE_TAG overridable)"
	@echo "  docker-push       - Push Docker image to registry"
	@echo "  helm-lint         - Lint the Helm chart"
