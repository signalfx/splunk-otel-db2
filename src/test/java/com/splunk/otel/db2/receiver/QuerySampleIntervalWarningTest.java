package com.splunk.otel.db2.receiver;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.splunk.otel.db2.config.Db2ReceiverConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Asserts the advisory warning is actually emitted, once, and names the database.
 *
 * <p>The previous test only checked {@code isBelowRecommendedInterval()}, which proves the
 * predicate but not that anything logs. The warning also used to live in config validation,
 * where it missed every caller that builds a {@link Db2Receiver} or Db2Pipeline from a
 * {@link Db2ReceiverConfig} directly rather than through {@code Db2Config.load()}.
 */
class QuerySampleIntervalWarningTest {

    private ListAppender<ILoggingEvent> appender;
    private ch.qos.logback.classic.Logger receiverLogger;

    @BeforeEach
    void attachAppender() {
        LoggerContext ctx = (LoggerContext) LoggerFactory.getILoggerFactory();
        receiverLogger = ctx.getLogger(Db2Receiver.class);
        appender = new ListAppender<>();
        appender.setContext(ctx);
        appender.start();
        receiverLogger.addAppender(appender);
    }

    @AfterEach
    void detachAppender() {
        receiverLogger.detachAppender(appender);
        appender.stop();
    }

    private List<String> warningsFor(String interval) {
        Db2ReceiverConfig config = new Db2ReceiverConfig();
        config.setHostname("db2.example.com");
        config.setDatabase("PROD");
        config.setUsername("u");
        config.setPassword("p");
        config.getEvents().getDbServerQuerySample().setEnabled(true);
        config.getQuerySampleCollection().setCollectionInterval(interval);

        // Scheduling opens a connection, which cannot succeed here. The warning is emitted
        // before that, so drive the scheduler and ignore the connection failure.
        try {
            new Db2Receiver(config, (io.opentelemetry.api.metrics.Meter) null,
                (io.opentelemetry.api.logs.Logger) null).startQuerySampleScheduler(11);
        } catch (RuntimeException ignored) {
            // expected: no Db2 to connect to
        }

        return appender.list.stream()
            .filter(e -> e.getLevel() == Level.WARN)
            .map(ILoggingEvent::getFormattedMessage)
            .filter(m -> m.contains("below the recommended"))
            .collect(Collectors.toList());
    }

    @Test
    void warnsExactlyOnceBelowTheRecommendedInterval() {
        List<String> warnings = warningsFor("1s");

        assertThat(warnings).hasSize(1);
        assertThat(warnings.get(0))
            .contains("is 1s")
            .contains("recommended 5s")
            .contains("PROD");
    }

    @Test
    void doesNotWarnAtOrAboveTheRecommendedInterval() {
        assertThat(warningsFor("5s")).isEmpty();
        assertThat(warningsFor("30s")).isEmpty();
    }
}
