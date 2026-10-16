package com.splunk.otel.db2.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Loads the example configs under testdata/ and asserts they mean what they say.
 *
 * <p>README's quick start tells readers to copy these files, so a key the parser does not
 * read is worse than a typo: the receiver starts, reports the feature inactive, and nothing
 * points back at the config. That is what {@code query_sample_enabled: true} did — it sat
 * beside the nested schema that had replaced it, and left query sampling off.
 *
 * <p>Asserting effective values alone would not have caught it, because some of the ignored
 * flat keys carried the same value as the default they were being ignored in favour of
 * ({@code query_sample_interval: "10s"} against a 10s default). So the key names are checked
 * against the set the parser actually reads, as well as the resulting behaviour.
 */
class ShippedConfigExamplesTest {

    /** Receiver keys {@link Db2Config} reads. Anything else at this level is ignored. */
    private static final Set<String> RECEIVER_KEYS = Set.of(
        "hostname", "hostport", "username", "password", "database",
        "collection_interval", "connect_timeout_seconds", "use_snapshot_monitor",
        "tls", "events", "top_query_collection", "query_sample_collection",
        "resource_attributes", "service_name", "service_namespace", "service_instance_id");

    private static final Map<String, Set<String>> NESTED_KEYS = Map.of(
        "top_query_collection", Set.of("collection_interval", "top_query_count",
            "max_query_sample_count", "max_explain_each_interval", "query_plan_cache_size",
            "query_plan_cache_ttl", "explain_schema"),
        "query_sample_collection", Set.of("collection_interval", "max_rows_per_query"),
        "events", Set.of("db.server.top_query", "db.server.query_sample"),
        "tls", Set.of("insecure", "insecure_skip_verify", "ca_file", "trust_store",
            "trust_store_password", "trust_store_type"));

    private static Path example(String name) {
        return Path.of("testdata", name);
    }

    @Test
    @SuppressWarnings("unchecked")
    void everyKeyInTheExamplesIsOneTheParserReads() throws Exception {
        for (String name : List.of("config.yaml", "docker-config.yaml")) {
            Yaml yaml = new Yaml(new SafeConstructor(new LoaderOptions()));
            Map<String, Object> doc = yaml.load(Files.readString(example(name)));
            Map<String, Object> receivers = (Map<String, Object>) doc.get("receivers");
            assertTrue(receivers != null && !receivers.isEmpty(), name + " declares no receivers");

            for (Map.Entry<String, Object> receiver : receivers.entrySet()) {
                Map<String, Object> settings = (Map<String, Object>) receiver.getValue();
                for (Map.Entry<String, Object> setting : settings.entrySet()) {
                    String key = setting.getKey();
                    assertTrue(RECEIVER_KEYS.contains(key),
                        name + ": " + receiver.getKey() + "." + key + " is not read by the "
                            + "parser, so it would be silently ignored");
                    Set<String> allowed = NESTED_KEYS.get(key);
                    if (allowed == null || !(setting.getValue() instanceof Map)) {
                        continue;
                    }
                    for (Object nested : ((Map<String, Object>) setting.getValue()).keySet()) {
                        assertTrue(allowed.contains(String.valueOf(nested)),
                            name + ": " + key + "." + nested + " is not read by the parser, "
                                + "so it would be silently ignored");
                    }
                }
            }
        }
    }

    @Test
    void theSingleDatabaseExampleEnablesWhatItClaims(@TempDir Path dir) throws Exception {
        Db2ReceiverConfig rc = loadFirst("config.yaml", dir);

        assertTrue(rc.getEvents().getDbServerQuerySample().isEnabled(),
            "the example enables query sampling, so it must actually be on");
        assertFalse(rc.getEvents().getDbServerTopQuery().isEnabled(),
            "the example leaves top queries off");

        assertEquals(250, rc.getTopQueryCollection().getTopQueryCount());
        assertEquals(1000, rc.getTopQueryCollection().getMaxQuerySampleCount());
        assertEquals(Duration.ofSeconds(60),
            rc.getTopQueryCollection().getCollectionIntervalDuration());
        assertEquals(Duration.ofSeconds(10),
            rc.getQuerySampleCollection().getCollectionIntervalDuration());
        assertEquals(50001, rc.getPort(), "TLS is on, so this must not be the cleartext port");
        assertFalse(rc.getTls().isInsecure(), "the example keeps TLS on");
    }

    /**
     * Loads a shipped example with the {@code ${env:...}} credential references replaced, so
     * the assertions are about the file rather than about the test's environment. Everything
     * else in the file is loaded exactly as shipped.
     */
    private static Db2ReceiverConfig loadFirst(String name, Path dir) throws Exception {
        String raw = Files.readString(example(name))
            .replace("${env:DB2_USER}", "u")
            .replace("${env:DB2_PASSWORD}", "p");
        Path copy = dir.resolve(name);
        Files.writeString(copy, raw);
        return Db2Config.load(copy.toString()).getReceivers().get(0);
    }


}
