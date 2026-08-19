package com.splunk.otel.db2.codegen;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * Generates Db2Metrics.java from metadata.yaml.
 *
 * Usage: MetricsGenerator <metadata.yaml> <output-dir>
 *
 * Each metric entry in metadata.yaml produces a static buildObserver() method
 * that registers the correct OTel instrument type (counter, gauge, upDownCounter).
 */
public class MetricsGenerator {

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

        @SuppressWarnings("unchecked")
        Map<String, Object> metrics = (Map<String, Object>) root.get("metrics");
        if (metrics == null) throw new IllegalStateException("No 'metrics' section in " + yamlPath);

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

            String instrument   = (String) def.get("instrument");
            String description  = (String) def.get("description");
            String unit         = (String) def.get("unit");

            String methodName   = toCamelCase(metricName);
            String returnType   = isDoubleInstrument(instrument) ? "ObservableDoubleMeasurement" : "ObservableLongMeasurement";

            sb.append("    /** ").append(description).append(" ({@code ").append(unit).append("}) */\n");
            sb.append("    public static ").append(returnType).append(" ").append(methodName).append("(Meter meter) {\n");
            sb.append("        return meter.").append(builderCall(instrument)).append("(\"").append(metricName).append("\")\n");
            // gaugeBuilder() → DoubleGaugeBuilder: long gauge needs .ofLongs()
            // counterBuilder() → LongCounterBuilder: double counter needs .ofDoubles()
            // upDownCounterBuilder() → LongUpDownCounterBuilder: already long, no conversion needed
            if (instrument.equals("long_gauge")) {
                sb.append("            .ofLongs()\n");
            } else if (instrument.equals("double_counter")) {
                sb.append("            .ofDoubles()\n");
            }
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

    private static String builderCall(String instrument) {
        switch (instrument) {
            case "long_counter":
            case "double_counter":   return "counterBuilder";
            case "long_updown_counter":
            case "double_updown_counter": return "upDownCounterBuilder";
            case "long_gauge":
            case "double_gauge":     return "gaugeBuilder";
            default: throw new IllegalArgumentException("Unknown instrument: " + instrument);
        }
    }

    private static boolean isDoubleInstrument(String instrument) {
        return instrument.startsWith("double_");
    }

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
