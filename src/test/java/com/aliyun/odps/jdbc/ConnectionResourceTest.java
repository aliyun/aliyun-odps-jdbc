package com.aliyun.odps.jdbc;

import java.io.FileInputStream;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.jdbc.utils.ConnectionResource;

public class ConnectionResourceTest {

  @Test
  public void testConnectionResourceTest() throws Exception {
    String odpsConfigFile = getClass().getClassLoader().getResource("odps_config.ini").getPath();
    String url1 =
        "jdbc:odps:http://1.1.1.1:8100/api?project=p1&loglevel=debug&accessId=123&accessKey=234%3D&logconffile=/Users/emerson/logback.xml&odps_config="
            + odpsConfigFile;
    ConnectionResource resource = new ConnectionResource(url1, null);
    Assert.assertEquals("http://1.1.1.1:8100/api", resource.getEndpoint());
    Assert.assertEquals("p2", resource.getProject());
    Assert.assertEquals("345", resource.getAccessId());
    Assert.assertEquals("456=", resource.getAccessKey());
    Assert.assertEquals(null, resource.getLogLevel());
    Assert.assertEquals("2", resource.getLifecycle());
    Assert.assertEquals("UTF-8", resource.getCharset());
    Assert.assertEquals("logback1.xml", resource.getLogConfFile());
    Assert.assertEquals(null, resource.getLogview());



    String logConfigFile = getClass().getClassLoader().getResource("logback.xml").getPath();
    String url2 =
        "jdbc:odps:http://1.1.1.1:8100/api?project=p1&loglevel=debug&accessId=123&accessKey=234%3D&logview_host=http://abc.com:8080&logconffile="
            + logConfigFile;
    resource = new ConnectionResource(url2, null);
    Assert.assertEquals("http://1.1.1.1:8100/api", resource.getEndpoint());
    Assert.assertEquals("p1", resource.getProject());
    Assert.assertEquals("123", resource.getAccessId());
    Assert.assertEquals("234=", resource.getAccessKey());
    Assert.assertEquals(logConfigFile, resource.getLogConfFile());
    Assert.assertEquals("debug", resource.getLogLevel());
    Assert.assertEquals("3", resource.getLifecycle());
    Assert.assertEquals("UTF-8", resource.getCharset());
    Assert.assertEquals(logConfigFile, resource.getLogConfFile());
    Assert.assertEquals("http://abc.com:8080", resource.getLogview());


    Properties info = new Properties();
    info.load(new FileInputStream(odpsConfigFile));
    resource = new ConnectionResource(url2, info);
    Assert.assertEquals("http://1.1.1.1:8100/api", resource.getEndpoint());
    Assert.assertEquals("p2", resource.getProject());
    Assert.assertEquals("345", resource.getAccessId());
    Assert.assertEquals("456=", resource.getAccessKey());
    Assert.assertEquals("debug", resource.getLogLevel());
    Assert.assertEquals("2", resource.getLifecycle());
    Assert.assertEquals("UTF-8", resource.getCharset());
    Assert.assertEquals("logback1.xml", resource.getLogConfFile());
    Assert.assertEquals("http://abc.com:8080", resource.getLogview());

  }


}
