package com.splunk.otel.db2.codegen;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * Reads metadata.yaml (otel-contrib schema) and generates Db2Metrics.java.
 *
 * Instrument block mapping:
 *   gauge { value_type: int }    → gaugeBuilder().ofLongs()
 *   gauge { value_type: double } → gaugeBuilder()
 *   sum   { value_type: int,    monotonic: true  } → counterBuilder()
 *   sum   { value_type: int,    monotonic: false } → upDownCounterBuilder()
 *   sum   { value_type: double, monotonic: true  } → counterBuilder().ofDoubles()
 *   sum   { value_type: double, monotonic: false } → upDownCounterBuilder().ofDoubles()
 */
public class MetricsGenerator {

    private static final Set<String> VALID_INSTRUMENT_BLOCKS = new HashSet<>(Arrays.asList("gauge", "sum", "histogram"));
    private static final Set<String> VALID_VALUE_TYPES       = new HashSet<>(Arrays.asList("int", "double"));
    private static final Set<String> VALID_AGGREGATIONS      = new HashSet<>(Arrays.asList("cumulative", "delta"));
    private static final Set<String> VALID_ATTR_TYPES        = new HashSet<>(Arrays.asList("string", "int", "double", "bool", "bytes", "slice", "map"));
    private static final Set<String> VALID_REQ_LEVELS        = new HashSet<>(Arrays.asList("required", "recommended", "opt_in"));

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: MetricsGenerator <metadata.yaml> <output-dir>");
            System.exit(1);
        }
        String yamlPath  = args[0];
        String outputDir = args[1];

        Yaml yaml = new Yaml(new SafeConstructor(new LoaderOptions()));
        Map<String, Object> root;
        try (InputStream is = new FileInputStream(yamlPath)) {
            root = yaml.load(is);
        }

        validate(root, yamlPath);

        @SuppressWarnings("unchecked")
        Map<String, Object> metrics = (Map<String, Object>) root.get("metrics");

        String pkg  = "com.splunk.otel.db2.generated";
        String cls  = "Db2Metrics";
        Path outDir = Paths.get(outputDir, pkg.replace('.', '/'));
        Files.createDirectories(outDir);

        StringBuilder sb = new StringBuilder();
        sb.append("package ").append(pkg).append(";\n\n");
        sb.append("// AUTO-GENERATED — do not edit. Modify metadata.yaml and run ./gradlew generateMetrics\n\n");
        sb.append("import io.opentelemetry.api.metrics.Meter;\n");
        sb.append("import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;\n");
        sb.append("import io.opentelemetry.api.metrics.ObservableLongMeasurement;\n\n");
        sb.append("public final class ").append(cls).append(" {\n");
        sb.append("    private ").append(cls).append("() {}\n\n");

        for (Map.Entry<String, Object> entry : metrics.entrySet()) {
            String metricName = entry.getKey();
            @SuppressWarnings("unchecked")
            Map<String, Object> def = (Map<String, Object>) entry.getValue();

            Boolean enabled = (Boolean) def.get("enabled");
            if (Boolean.FALSE.equals(enabled)) continue; // skip disabled metrics

            String description = (String) def.get("description");
            String unit        = (String) def.get("unit");

            InstrumentSpec spec = resolveInstrument(metricName, def);
            String methodName   = toCamelCase(metricName);

            sb.append("    /** ").append(description).append(" ({@code ").append(unit).append("}) */\n");
            sb.append("    public static ").append(spec.returnType).append(" ").append(methodName).append("(Meter meter) {\n");
            sb.append("        return meter.").append(spec.builderMethod).append("(\"").append(metricName).append("\")\n");
            if (spec.ofDoubles)   sb.append("            .ofDoubles()\n");
            else if (spec.ofLongs) sb.append("            .ofLongs()\n");
            sb.append("            .setDescription(\"").append(escapeJava(description)).append("\")\n");
            sb.append("            .setUnit(\"").append(escapeJava(unit)).append("\")\n");
            sb.append("            .buildObserver();\n");
            sb.append("    }\n\n");
        }

        sb.append("}\n");

        Path outFile = outDir.resolve(cls + ".java");
        Files.writeString(outFile, sb.toString());
        System.out.println("Generated: " + outFile);
    }

    // ── validation ────────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private static void validate(Map<String, Object> root, String path) {
        List<String> errors = new ArrayList<>();

        if (!root.containsKey("type"))   errors.add("Missing required field: type");
        if (!root.containsKey("status")) errors.add("Missing required field: status");

        Map<String, Object> attrs = (Map<String, Object>) root.get("attributes");
        if (attrs != null) {
            for (Map.Entry<String, Object> e : attrs.entrySet()) {
                Map<String, Object> a = (Map<String, Object>) e.getValue();
                String type = (String) a.get("type");
                if (type == null) errors.add("attributes." + e.getKey() + ": missing 'type'");
                else if (!VALID_ATTR_TYPES.contains(type))
                    errors.add("attributes." + e.getKey() + ": unknown type '" + type + "'. Valid: " + VALID_ATTR_TYPES);
                String reqLevel = (String) a.get("requirement_level");
                if (reqLevel != null && !VALID_REQ_LEVELS.contains(reqLevel))
                    errors.add("attributes." + e.getKey() + ": unknown requirement_level '" + reqLevel + "'");
            }
        }

        Map<String, Object> metrics = (Map<String, Object>) root.get("metrics");
        if (metrics == null) {
            errors.add("Missing required section: metrics");
        } else {
            for (Map.Entry<String, Object> e : metrics.entrySet()) {
                String name = e.getKey();
                Map<String, Object> def = (Map<String, Object>) e.getValue();
                if (def.get("description") == null) errors.add("metrics." + name + ": missing 'description'");
                if (def.get("unit")        == null) errors.add("metrics." + name + ": missing 'unit'");

                // exactly one instrument block
                List<String> found = new ArrayList<>();
                for (String b : VALID_INSTRUMENT_BLOCKS) if (def.containsKey(b)) found.add(b);
                if (found.isEmpty())
                    errors.add("metrics." + name + ": must have exactly one of: " + VALID_INSTRUMENT_BLOCKS);
                else if (found.size() > 1)
                    errors.add("metrics." + name + ": multiple instrument blocks: " + found);
                else {
                    String block = found.get(0);
                    Map<String, Object> inst = (Map<String, Object>) def.get(block);
                    if (inst == null) errors.add("metrics." + name + "." + block + ": must be a map");
                    else {
                        String vt = (String) inst.get("value_type");
                        if (vt == null) errors.add("metrics." + name + "." + block + ": missing 'value_type'");
                        else if (!VALID_VALUE_TYPES.contains(vt))
                            errors.add("metrics." + name + "." + block + ": unknown value_type '" + vt + "'");

                        if (block.equals("sum")) {
                            if (!inst.containsKey("monotonic"))
                                errors.add("metrics." + name + ".sum: missing 'monotonic'");
                            String agg = (String) inst.get("aggregation_temporality");
                            if (agg != null && !VALID_AGGREGATIONS.contains(agg))
                                errors.add("metrics." + name + ".sum: unknown aggregation_temporality '" + agg + "'");
                        }
                    }
                }
            }
        }

        if (!errors.isEmpty()) {
            System.err.println("metadata.yaml validation failed (" + path + "):");
            errors.forEach(err -> System.err.println("  - " + err));
            System.exit(1);
        }
    }

    // ── instrument resolution ──────────────────────────────────────────────────

    private static class InstrumentSpec {
        String returnType;
        String builderMethod;
        boolean ofDoubles;
        boolean ofLongs;
    }

    @SuppressWarnings("unchecked")
    private static InstrumentSpec resolveInstrument(String metricName, Map<String, Object> def) {
        InstrumentSpec s = new InstrumentSpec();
        if (def.containsKey("gauge")) {
            Map<String, Object> g = (Map<String, Object>) def.get("gauge");
            boolean isDouble = "double".equals(g.get("value_type"));
            s.returnType    = isDouble ? "ObservableDoubleMeasurement" : "ObservableLongMeasurement";
            s.builderMethod = "gaugeBuilder";
            s.ofLongs       = !isDouble; // gaugeBuilder() starts as Double; need ofLongs() for int
        } else if (def.containsKey("sum")) {
            Map<String, Object> sm = (Map<String, Object>) def.get("sum");
            boolean isDouble  = "double".equals(sm.get("value_type"));
            boolean monotonic = Boolean.TRUE.equals(sm.get("monotonic"));
            s.returnType    = isDouble ? "ObservableDoubleMeasurement" : "ObservableLongMeasurement";
            s.builderMethod = monotonic ? "counterBuilder" : "upDownCounterBuilder";
            // counterBuilder() starts as Long; upDownCounterBuilder() starts as Long
            // only need ofDoubles() for double sums
            s.ofDoubles = isDouble;
        } else {
            throw new IllegalArgumentException("metrics." + metricName + ": no supported instrument block");
        }
        return s;
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private static String toCamelCase(String metricName) {
        // db2.buffer_pool.hit_ratio → bufferPoolHitRatio
        String[] parts = metricName.replaceFirst("^db2\\.", "").split("[._]");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            String p = parts[i];
            if (p.isEmpty()) continue;
            sb.append(i == 0 ? p : Character.toUpperCase(p.charAt(0)) + p.substring(1));
        }
        return sb.toString();
    }

    private static String escapeJava(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
