package com.aliyun.odps.jdbc.utils;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.tunnel.InstanceTunnel;
import java.net.URI;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class InstanceDataIteratorTest {

  @Test
  public void createInstanceTunnelAppliesTunnelQuotaName() throws Exception {
    Odps odps = Mockito.mock(Odps.class);
    InstanceTunnel tunnel = InstanceDataIterator.createInstanceTunnel(odps, "tunnel_quota");

    Assertions.assertEquals(
        "tunnel_quota",
        ((com.aliyun.odps.tunnel.Configuration) tunnel.getConfig()).getQuotaName());
  }

  @Test
  public void createInstanceTunnelIgnoresEmptyTunnelQuotaName() {
    Odps odps = Mockito.mock(Odps.class);

    InstanceTunnel tunnel = InstanceDataIterator.createInstanceTunnel(odps, "");

    Assertions.assertFalse(
        ((com.aliyun.odps.tunnel.Configuration) tunnel.getConfig()).availableQuotaName());
  }

  @Test
  public void explicitEndpointOverridesInheritedEndpointAndPreservesQuota() throws Exception {
    Odps odps = new Odps(new AliyunAccount("fixture-id", "fixture-key"));
    odps.setTunnelEndpoint("http://inherited.invalid");

    InstanceTunnel tunnel = InstanceDataIterator.createInstanceTunnel(
        odps, "tunnel_quota", "http://configured.invalid");

    Assertions.assertEquals(URI.create("http://configured.invalid"),
                            tunnel.getConfig().getEndpoint("project"));
    Assertions.assertEquals("tunnel_quota",
        ((com.aliyun.odps.tunnel.Configuration) tunnel.getConfig()).getQuotaName());
    Assertions.assertEquals("http://inherited.invalid", odps.getTunnelEndpoint());
  }

  @Test
  public void missingEndpointKeepsLegacyInheritedEndpoint() throws Exception {
    Odps odps = new Odps(new AliyunAccount("fixture-id", "fixture-key"));
    odps.setTunnelEndpoint("http://inherited.invalid");

    for (String endpoint : new String[] {null, ""}) {
      InstanceTunnel tunnel = InstanceDataIterator.createInstanceTunnel(odps, null, endpoint);
      Assertions.assertEquals(URI.create("http://inherited.invalid"),
                              tunnel.getConfig().getEndpoint("project"));
    }
    Assertions.assertEquals(URI.create("http://inherited.invalid"),
        InstanceDataIterator.createInstanceTunnel(odps, null).getConfig().getEndpoint("project"));
  }

  @Test
  public void endpointRemainsLocalToEachTunnel() throws Exception {
    Odps odps = new Odps(new AliyunAccount("fixture-id", "fixture-key"));
    InstanceTunnel first = InstanceDataIterator.createInstanceTunnel(
        odps, null, "http://first.invalid");
    InstanceTunnel second = InstanceDataIterator.createInstanceTunnel(
        odps, null, "http://second.invalid");

    Assertions.assertEquals(URI.create("http://first.invalid"),
                            first.getConfig().getEndpoint("project"));
    Assertions.assertEquals(URI.create("http://second.invalid"),
                            second.getConfig().getEndpoint("project"));
    Assertions.assertNull(odps.getTunnelEndpoint());
  }
}
