package com.splunk.otel.db2.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.regex.Pattern;

/**
 * Faithful port of sqlserverreceiver isLocalhost / computeServiceInstanceID (upstream PR #49886).
 *
 * Key invariant: isLoopback() NEVER triggers DNS resolution. It only matches:
 *   - the literal name "localhost" (case-insensitive)
 *   - a blank/null host (co-located sidecar with no hostname set)
 *   - an IPv4 or IPv6 literal whose parsed address is a loopback (127.x.x.x, ::1)
 * Hostnames like "db2-prod.example.com" or "localhost.corp" are never resolved and always
 * return false, matching Go's net.ParseIP behaviour.
 */
public final class ResourceIdentity {
    private static final Logger log = LoggerFactory.getLogger(ResourceIdentity.class);

    // Matches IP literals without triggering DNS:
    //   branch 1 — IPv6 (hex + at least one colon, optional brackets): ::1, [::1], 2001:db8::1
    //   branch 2 — IPv4-mapped IPv6 (::ffff:a.b.c.d): handles dual-stack loopback ::ffff:127.0.0.1
    //   branch 3 — IPv4 (four dotted decimal octets): 127.0.0.1
    // Branch 1 requires at least one colon so bare hex words ("db", "beef") never match.
    private static final Pattern IP_LITERAL = Pattern.compile(
        "^(\\[?[0-9a-fA-F]*:[0-9a-fA-F:]*]?)$" +
        "|^::ffff:\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}$" +
        "|^(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})$",
        Pattern.CASE_INSENSITIVE);

    private ResourceIdentity() {}

    /**
     * True when host is null/blank, "localhost", or an IP literal whose address isLoopback().
     * Never resolves DNS.
     */
    public static boolean isLoopback(String host) {
        if (host == null || host.isBlank()) return true;
        if (host.equalsIgnoreCase("localhost")) return true;
        InetAddress addr = parseIpLiteral(host);
        return addr != null && addr.isLoopbackAddress();
    }

    /**
     * Parse an IPv4/IPv6 literal without triggering DNS. Returns null for hostnames.
     * Strips IPv6 brackets before parsing.
     */
    static InetAddress parseIpLiteral(String host) {
        String h = (host.startsWith("[") && host.endsWith("]"))
            ? host.substring(1, host.length() - 1)
            : host;
        if (!IP_LITERAL.matcher(h).matches()) return null;
        try {
            // InetAddress.getByName does NOT do DNS for IP literals — safe here
            return InetAddress.getByName(h);
        } catch (UnknownHostException e) {
            return null;
        }
    }

    /**
     * Host to use for identity attributes (service.instance.id, server.address).
     * Remote targets are returned unchanged.
     * Loopback/unset targets are replaced with the collector host's own name so that
     * co-located sidecars produce unique identities across the fleet.
     * On lookup failure, falls back to the configured host and warns — never throws.
     */
    public static String resolveIdentityHost(String configuredHost) {
        if (!isLoopback(configuredHost)) return configuredHost;
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            log.warn("Could not resolve local hostname for loopback target '{}'; " +
                     "service.instance.id may collide across co-located sidecars: {}",
                     configuredHost, e.getMessage());
            return configuredHost != null ? configuredHost : "localhost";
        }
    }
}
