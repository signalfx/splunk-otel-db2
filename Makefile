.PHONY: build test integration-test run jar clean help

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

help:
	@echo "Available targets:"
	@echo "  build             - Clean and build (unit tests included)"
	@echo "  test              - Run unit tests"
	@echo "  integration-test  - Run integration tests (requires Docker)"
	@echo "  run               - Run the receiver locally (CONFIG=<path> required)"
	@echo "  jar               - Build fat jar"
	@echo "  clean             - Clean build outputs"
