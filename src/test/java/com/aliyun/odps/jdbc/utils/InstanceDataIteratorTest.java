package com.aliyun.odps.jdbc.utils;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.tunnel.InstanceTunnel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class InstanceDataIteratorTest {

  @Test
  public void createInstanceTunnelAppliesTunnelQuotaName() throws Exception {
    Odps odps = Mockito.mock(Odps.class);
    Instance instance = Mockito.mock(Instance.class);

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
}
