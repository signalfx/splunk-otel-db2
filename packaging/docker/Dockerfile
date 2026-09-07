# Stage 1 — build the fat jar
FROM eclipse-temurin:17-jdk-alpine AS builder
WORKDIR /build
COPY . .
RUN ./gradlew shadowJar --no-daemon -q

# Stage 2 — carve a minimal custom JRE with jlink
# Modules determined via: jdeps --list-deps --ignore-missing-deps splunk-otel-db2-*.jar
FROM eclipse-temurin:17-jdk-alpine AS jlink
RUN $JAVA_HOME/bin/jlink \
      --add-modules java.base,java.desktop,java.logging,java.management,java.naming,java.rmi,java.security.jgss,java.sql,java.transaction.xa,java.xml,jdk.unsupported \
      --strip-debug \
      --no-man-pages \
      --no-header-files \
      --compress=2 \
      --output /custom-jre

# Stage 3 — minimal runtime image (~70 MB)
FROM alpine:3.20
RUN addgroup -g 10001 splunk-otel-db2 \
 && adduser  -u 10001 -G splunk-otel-db2 -s /sbin/nologin -D splunk-otel-db2
COPY --from=jlink  /custom-jre              /opt/jre
COPY --from=builder /build/build/libs/splunk-otel-db2-*.jar /app/splunk-otel-db2.jar
USER 10001
ENTRYPOINT ["/opt/jre/bin/java", "-jar", "/app/splunk-otel-db2.jar"]
CMD ["/etc/splunk-otel-db2/config.yaml"]
