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
  public void createInstanceTunnelAppliesConnectionTunnelTimeouts() {
    Odps odps = new Odps(new AliyunAccount("fixture-id", "fixture-key"));

    InstanceTunnel tunnel = InstanceDataIterator.createInstanceTunnel(
        odps, null, "http://configured.invalid", 7, 30);

    Assertions.assertEquals(7, tunnel.getConfig().getSocketConnectTimeout());
    Assertions.assertEquals(30, tunnel.getConfig().getSocketTimeout());
  }

  @Test
  public void createInstanceTunnelKeepsSdkDefaultsWhenTimeoutsAreUnsetOrNegative() {
    Odps odps = new Odps(new AliyunAccount("fixture-id", "fixture-key"));

    // -1 is "not configured": the SDK default stays in place, and so it does on the overloads
    // that carry no timeout argument at all.
    InstanceTunnel negative = InstanceDataIterator.createInstanceTunnel(
        odps, null, "http://configured.invalid", -1, -1);
    InstanceTunnel legacy = InstanceDataIterator.createInstanceTunnel(
        odps, null, "http://configured.invalid");
    for (InstanceTunnel tunnel : new InstanceTunnel[] {negative, legacy}) {
      Assertions.assertEquals(com.aliyun.odps.commons.GeneralConfiguration
          .DEFAULT_SOCKET_CONNECT_TIMEOUT, tunnel.getConfig().getSocketConnectTimeout());
      Assertions.assertEquals(com.aliyun.odps.commons.GeneralConfiguration
          .DEFAULT_SOCKET_TIMEOUT, tunnel.getConfig().getSocketTimeout());
    }
  }

  @Test
  public void createInstanceTunnelHandsOverZeroAndGatesEachOptionSeparately() {
    Odps odps = new Odps(new AliyunAccount("fixture-id", "fixture-key"));

    // 0 is a configured value meaning "no SDK-imposed bound", not "unset".
    InstanceTunnel zero = InstanceDataIterator.createInstanceTunnel(
        odps, null, "http://configured.invalid", 0, 0);
    Assertions.assertEquals(0, zero.getConfig().getSocketConnectTimeout());
    Assertions.assertEquals(0, zero.getConfig().getSocketTimeout());

    // Setting only one of the two must not drag the other off its default.
    InstanceTunnel readOnly = InstanceDataIterator.createInstanceTunnel(
        odps, null, "http://configured.invalid", -1, 30);
    Assertions.assertEquals(com.aliyun.odps.commons.GeneralConfiguration
        .DEFAULT_SOCKET_CONNECT_TIMEOUT, readOnly.getConfig().getSocketConnectTimeout());
    Assertions.assertEquals(30, readOnly.getConfig().getSocketTimeout());
  }

  @Test
  public void timeoutsDoNotLeakIntoOtherTunnelClients() {
    Odps odps = new Odps(new AliyunAccount("fixture-id", "fixture-key"));

    InstanceTunnel configured = InstanceDataIterator.createInstanceTunnel(
        odps, null, "http://first.invalid", 7, 30);
    InstanceTunnel untouched = new InstanceTunnel(odps);

    Assertions.assertEquals(7, configured.getConfig().getSocketConnectTimeout());
    Assertions.assertEquals(30, configured.getConfig().getSocketTimeout());
    Assertions.assertEquals(com.aliyun.odps.commons.GeneralConfiguration
        .DEFAULT_SOCKET_CONNECT_TIMEOUT, untouched.getConfig().getSocketConnectTimeout());
    Assertions.assertEquals(com.aliyun.odps.commons.GeneralConfiguration
        .DEFAULT_SOCKET_TIMEOUT, untouched.getConfig().getSocketTimeout());
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
