package com.splunk.otel.db2.util;

/**
 * Retained for backward compatibility. Delegates to ResourceIdentity.
 * Use ResourceIdentity directly in new code.
 */
public class HostNormalizer {
    private HostNormalizer() {}

    public static String resolveServerAddress(String host) {
        return ResourceIdentity.resolveIdentityHost(host);
    }

    public static boolean isLoopback(String host) {
        return ResourceIdentity.isLoopback(host);
    }
}
