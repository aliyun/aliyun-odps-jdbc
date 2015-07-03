package com.aliyun.odps.jdbc.impl;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.security.SecurityManager;
import com.aliyun.odps.utils.StringUtils;

public class LogView {

  private static final String POLICY_TYPE = "BEARER";
  private static final String HOST_DEFAULT = "http://webconsole.odps.aliyun-inc.com:8080";
  private String logViewHost = HOST_DEFAULT;

  Odps odps;

  public LogView(Odps odps) {
    this.odps = odps;
    if (odps.getLogViewHost() != null) {
      logViewHost = odps.getLogViewHost();
    }
  }

  public String getLogViewHost() {
    return logViewHost;
  }

  public void setLogViewHost(String logViewHost) {
    this.logViewHost = logViewHost;
  }

  public String generateLogView(Instance instance, long hours) throws OdpsException {
    if (StringUtils.isNullOrEmpty(logViewHost)) {
      return "";
    }

    SecurityManager sm = odps.projects().get(instance.getProject()).getSecurityManager();
    String policy = generatePolicy(instance, hours);
    String token = sm.generateAuthorizationToken(policy, POLICY_TYPE);
    String logview = logViewHost + "/logview/?h=" + odps.getEndpoint() + "&p="
        + instance.getProject() + "&i=" + instance.getId() + "&token=" + token;
    return logview;
  }

  private String generatePolicy(Instance instance, long hours) {
    String policy = "{\n" //
        + "    \"expires_in_hours\": " + String.valueOf(hours) + ",\n" //
        + "    \"policy\": {\n" + "        \"Statement\": [{\n"
        + "            \"Action\": [\"odps:Read\"],\n" + "            \"Effect\": \"Allow\",\n" //
        + "            \"Resource\": \"acs:odps:*:projects/" + instance.getProject() + "/instances/"
        + instance.getId() + "\"\n" //
        + "        }],\n"//
        + "        \"Version\": \"1\"\n" //
        + "    }\n" //
        + "}";
    return policy;
  }
}
