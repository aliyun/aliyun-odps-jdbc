/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */


package com.aliyun.odps.jdbc.utils;

import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Project;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.google.common.collect.ImmutableMap;

public class TestUtils {

  private static AtomicLong counter = new AtomicLong();
  private static Project project = null;
  private static String processName = ManagementFactory.getRuntimeMXBean().getName();
  private static String processID = processName.substring(0, processName.indexOf('@'));
  private static Random random = new Random();

  public static Project getProject(Odps odps) throws OdpsException {
    if (project == null) {
      project = odps.projects().get();
    }
    return project;
  }


  public static String getVersion() {
    return "tunnel_aliyunsdk_" + processID + "_";
  }

  public static String getRandomProjectName() {
    String projectName =
        "otopen_prj_" + processID + "_" + System.currentTimeMillis() + "_" + counter.addAndGet(1);
    System.out.println(projectName);
    return projectName;
  }

  public static String getRandomTableName() {
    return getVersion() + "table_" + System.currentTimeMillis() + "_" + counter.addAndGet(1);
  }

  public static String getRandomVolumeName() {
    return getVersion() + "volume_" + System.currentTimeMillis() + "_" + counter.addAndGet(1);
  }

  public static String getRandomPartitionName() {
    return "pt_" + System.currentTimeMillis() + "_" + counter.addAndGet(1);
  }

  public static String getRandomFileName() {
    return "file_" + System.currentTimeMillis() + "_" + counter.addAndGet(1);
  }

  public static float randomFloat() {
    return random.nextFloat();
  }

  public static double randomDouble() {
    return random.nextDouble();
  }

  public static byte[] randomBytes() {
    byte[] res = new byte[randomInt(200)];
    random.nextBytes(res);

    return res;
  }

  public static int randomInt() {
    return random.nextInt();
  }

  public static int randomInt(int bound) {
    // [1, bound]
    return random.nextInt(bound) + 1;
  }

  public static boolean randomBoolean() {
    return random.nextBoolean();
  }

  public static long randomLong() {
    return random.nextLong();
  }

  public static byte randomByte() {
    return (byte) random.nextInt(Byte.MAX_VALUE);
  }

  public static short randomShort() {
    return (short) random.nextInt(Short.MAX_VALUE);
  }

  public static String randomString() {
    return randomString(random.nextInt(200) + 1);
  }

  public static String randomString(int length) {
    StringBuilder builder = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      int a = random.nextInt(128);

      builder.append((char) (a));
    }

    return builder.toString();
  }


  public static java.util.Date randomDate() {
    return new java.util.Date(System.currentTimeMillis() + 86400000 * TestUtils.randomInt(100000));
  }

  public static Connection getConnection(Map<String, String> properties) throws Exception {
    Properties odpsConfig = new Properties();

    InputStream is =
        Thread.currentThread().getContextClassLoader().getResourceAsStream("conf.properties");
    odpsConfig.load(is);

    Class.forName("com.aliyun.odps.jdbc.OdpsDriver");

    String endpoint = odpsConfig.getProperty("end_point");
    String project = odpsConfig.getProperty("project_name");
    String username = odpsConfig.getProperty("access_id");
    String password = odpsConfig.getProperty("access_key");

    String url = String.format("jdbc:odps:%s?project=%s",
                               endpoint, project);
    StringBuilder sb = new StringBuilder(url);
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      sb.append("&").append(entry.getKey()).append("=").append(entry.getValue());
    }
    // pass project name via url
    return DriverManager.getConnection(sb.toString(), username, password);
  }

  public static Connection getConnection() throws Exception {
    return getConnection(Collections.emptyMap());
  }

  public static Connection getConnectionWithTimezone(String zoneId) throws Exception {
    return getConnection(ImmutableMap.of("timezone", zoneId));
  }

  public static Odps getOdps() throws Exception {
    Properties odpsConfig = new Properties();

    InputStream is =
        Thread.currentThread().getContextClassLoader().getResourceAsStream("conf.properties");
    odpsConfig.load(is);
    String endpoint = odpsConfig.getProperty("end_point");
    String project = odpsConfig.getProperty("project_name");
    String username = odpsConfig.getProperty("access_id");
    String password = odpsConfig.getProperty("access_key");

    Account account = new AliyunAccount(username, password);
    Odps odps = new Odps(account);
    odps.setDefaultProject(project);
    odps.setEndpoint(endpoint);
    return odps;
  }
}
