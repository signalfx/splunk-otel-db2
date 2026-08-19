package com.splunk.otel.db2.processor;

import io.opentelemetry.sdk.metrics.SdkMeterProviderBuilder;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.View;

public class Db2Processor {

    // Apply Views to the SdkMeterProviderBuilder — mirrors filterprocessor/transformprocessor
    public static SdkMeterProviderBuilder apply(SdkMeterProviderBuilder builder) {

        // Drop internal OTel SDK metrics (not DB2 metrics)
        builder.registerView(
            InstrumentSelector.builder()
                .setMeterName("io.opentelemetry.sdk.metrics")
                .build(),
            View.builder().setName("drop").build()
        );

        return builder;
    }
}
