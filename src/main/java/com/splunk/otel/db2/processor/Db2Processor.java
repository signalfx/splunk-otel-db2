package com.splunk.otel.db2.processor;

import io.opentelemetry.sdk.metrics.SdkMeterProviderBuilder;

public class Db2Processor {

    public static SdkMeterProviderBuilder apply(SdkMeterProviderBuilder builder) {
        // No views needed — OTel SDK does not export internal metrics by default.
        return builder;
    }
}
