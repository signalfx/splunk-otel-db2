.PHONY: build test integration-test run jar clean help

build:
	./gradlew clean build

test:
	./gradlew test

integration-test:
	./gradlew integrationTest

run:
	./gradlew run

jar:
	./gradlew jar

clean:
	./gradlew clean

help:
	@echo "Available targets:"
	@echo "  build             - Clean and build (unit tests included)"
	@echo "  test              - Run unit tests"
	@echo "  integration-test  - Run integration tests (requires Docker)"
	@echo "  run               - Run the receiver locally"
	@echo "  jar               - Build fat jar"
	@echo "  clean             - Clean build outputs"
