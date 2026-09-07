package com.splunk.otel.db2.pipeline;

import com.splunk.otel.db2.util.HostNormalizer;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.assertj.core.api.Assertions.assertThat;

class Db2PipelineTest {

    @Test
    void nonLoopbackHostPassedThrough() {
        assertThat(HostNormalizer.resolveServerAddress("db2-prod.example.com"))
            .isEqualTo("db2-prod.example.com");
    }

    @Test
    void localhostResolvedToMachineHostname() throws UnknownHostException {
        String resolved = HostNormalizer.resolveServerAddress("localhost");
        assertThat(resolved)
            .isNotEqualTo("localhost")
            .isEqualTo(InetAddress.getLocalHost().getHostName());
    }

    @Test
    void loopbackIpv4ResolvedToMachineHostname() throws UnknownHostException {
        String resolved = HostNormalizer.resolveServerAddress("127.0.0.1");
        assertThat(resolved)
            .isNotEqualTo("127.0.0.1")
            .isEqualTo(InetAddress.getLocalHost().getHostName());
    }

    @Test
    void loopbackIpv6ResolvedToMachineHostname() throws UnknownHostException {
        String resolved = HostNormalizer.resolveServerAddress("::1");
        assertThat(resolved)
            .isNotEqualTo("::1")
            .isEqualTo(InetAddress.getLocalHost().getHostName());
    }

    @Test
    void emptyHostResolvedToMachineHostname() throws UnknownHostException {
        String resolved = HostNormalizer.resolveServerAddress("");
        assertThat(resolved).isEqualTo(InetAddress.getLocalHost().getHostName());
    }

    @Test
    void nullHostResolvedToMachineHostname() throws UnknownHostException {
        String resolved = HostNormalizer.resolveServerAddress(null);
        assertThat(resolved).isEqualTo(InetAddress.getLocalHost().getHostName());
    }

    @Test
    void localhostCaseInsensitive() throws UnknownHostException {
        String resolved = HostNormalizer.resolveServerAddress("LOCALHOST");
        assertThat(resolved).isEqualTo(InetAddress.getLocalHost().getHostName());
    }
}
