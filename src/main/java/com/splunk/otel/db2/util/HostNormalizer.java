package com.splunk.otel.db2.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class HostNormalizer {
    private static final Logger log = LoggerFactory.getLogger(HostNormalizer.class);

    private HostNormalizer() {}

    // Mirrors sqlserverreceiver resolveServerEndpoint (PR #49886).
    public static String resolveServerAddress(String host) {
        if (host == null || host.isEmpty() || isLoopback(host)) {
            try {
                return InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                log.warn("Could not resolve local hostname for server.address, using configured value '{}': {}", host, e.getMessage());
                return host != null ? host : "localhost";
            }
        }
        return host;
    }

    public static boolean isLoopback(String host) {
        if (host.equalsIgnoreCase("localhost")) return true;
        try {
            return InetAddress.getByName(host).isLoopbackAddress();
        } catch (UnknownHostException e) {
            return false;
        }
    }
}
