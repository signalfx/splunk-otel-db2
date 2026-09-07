# Stage 1 — build the fat jar
FROM --platform=$BUILDPLATFORM eclipse-temurin:17-jdk AS builder
WORKDIR /build
COPY . .
RUN ./gradlew shadowJar --no-daemon -q

# Stage 2 — carve a minimal custom JRE with jlink
# Uses the same multi-arch Temurin image so the JRE matches the target platform.
# Modules determined via: jdeps --list-deps --ignore-missing-deps splunk-otel-db2-*.jar
FROM eclipse-temurin:17-jdk AS jlink
RUN $JAVA_HOME/bin/jlink \
      --add-modules java.base,java.desktop,java.logging,java.management,java.naming,java.rmi,java.security.jgss,java.sql,java.transaction.xa,java.xml,jdk.crypto.ec,jdk.unsupported \
      --strip-debug \
      --no-man-pages \
      --no-header-files \
      --compress=2 \
      --output /custom-jre

# Stage 3 — minimal glibc runtime (~120 MB); compatible with both Docker and systemd on Ubuntu/RHEL
FROM debian:12-slim
RUN groupadd -g 10001 splunk-otel-db2 \
 && useradd -u 10001 -g splunk-otel-db2 -s /sbin/nologin -M splunk-otel-db2
COPY --from=jlink  /custom-jre              /opt/jre
COPY --from=builder /build/build/libs/splunk-otel-db2-*.jar /app/splunk-otel-db2.jar
USER 10001
ENTRYPOINT ["/opt/jre/bin/java", "-Djava.io.tmpdir=/tmp", "-jar", "/app/splunk-otel-db2.jar"]
CMD ["/etc/splunk-otel-db2/config.yaml"]
