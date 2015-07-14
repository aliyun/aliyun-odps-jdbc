package com.aliyun.odps.jdbc;

import java.io.InputStream;
import java.util.Properties;

public class BVTConf {

  private static boolean inited = false;
  private static String accessId = null;
  private static String accessKey = null;
  private static String endPoint = null;
  private static String projectName = null;

  private static void init() {
    if (inited) {
      return;
    }

    try {
      InputStream is =
          Thread.currentThread().getContextClassLoader().getResourceAsStream("bvt_conf.properties");

      Properties property = new Properties();
      property.load(is);

      accessId = property.getProperty("access_id");
      accessKey = property.getProperty("access_key");
      endPoint = property.getProperty("end_point");
      projectName = property.getProperty("project_name");

      is.close();
    } catch (Exception ex) {
      ex.printStackTrace();
    }
    inited = true;
  }

  public static String getAccessId() {
    init();

    return accessId;
  }

  public static String getAccessKey() {
    init();

    return accessKey;
  }

  public static String getEndPoint() {
    init();

    return endPoint;
  }

  public static String getProjectName() {
    init();

    return projectName;
  }
}
