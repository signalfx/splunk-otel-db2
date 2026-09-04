package com.splunk.otel.db2.util;

import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.assertj.core.api.Assertions.assertThat;

class Db2ResourceIdentityTest {

    @Test
    void remoteHostUnchanged() {
        assertThat(ResourceIdentity.resolveIdentityHost("db2-prod-1.example.com"))
            .isEqualTo("db2-prod-1.example.com");
    }

    @Test
    void localhostResolved() throws UnknownHostException {
        String resolved = ResourceIdentity.resolveIdentityHost("localhost");
        assertThat(resolved)
            .isNotEqualTo("localhost")
            .isEqualTo(InetAddress.getLocalHost().getHostName());
    }

    @Test
    void loopbackIpResolved() throws UnknownHostException {
        String resolved = ResourceIdentity.resolveIdentityHost("127.0.0.1");
        assertThat(resolved)
            .isNotEqualTo("127.0.0.1")
            .isEqualTo(InetAddress.getLocalHost().getHostName());
    }

    @Test
    void ipv6LoopbackResolved() throws UnknownHostException {
        String resolved = ResourceIdentity.resolveIdentityHost("::1");
        assertThat(resolved)
            .isNotEqualTo("::1")
            .isEqualTo(InetAddress.getLocalHost().getHostName());
    }

    @Test
    void blankTreatedAsLoopback() throws UnknownHostException {
        String resolved = ResourceIdentity.resolveIdentityHost("");
        assertThat(resolved).isEqualTo(InetAddress.getLocalHost().getHostName());
    }

    @Test
    void hostnameNeverParsedAsIp() {
        // "localhost.corp" must not be treated as a loopback — it's not an IP literal
        // and must never trigger DNS. isLoopback() should return false.
        assertThat(ResourceIdentity.isLoopback("localhost.corp")).isFalse();
        // resolveIdentityHost must return it unchanged (no DNS, no substitution)
        assertThat(ResourceIdentity.resolveIdentityHost("localhost.corp"))
            .isEqualTo("localhost.corp");
    }
}
