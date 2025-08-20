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

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.Map;
import java.util.Random;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.account.StsAccount;
import com.aliyun.odps.utils.StringUtils;
import com.google.common.collect.ImmutableMap;

public class TestUtils {

    private static Random random = new Random();

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
        return new java.util.Date(
            System.currentTimeMillis() + 86400000 * TestUtils.randomInt(100000));
    }

    public static Connection getConnection(Map<String, String> properties) throws Exception {

        Class.forName("com.aliyun.odps.jdbc.OdpsDriver");

        String endpoint = System.getenv("odps_endpoint");
        String project = System.getenv("MAXCOMPUTE_PROJECT");
        String accessId = System.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID");
        String accessKey = System.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET");
        String stsToken = System.getenv("ALIBABA_CLOUD_SECURITY_TOKEN");

        String url = String.format("jdbc:odps:%s?project=%s&accessId=%s&accessKey=%s",
                                   endpoint, project, accessId, accessKey);
        if (!StringUtils.isBlank(stsToken)) {
            url += "&stsToken=" + stsToken;
        }
        StringBuilder sb = new StringBuilder(url);
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            sb.append("&").append(entry.getKey()).append("=").append(entry.getValue());
        }
        // pass project name via url
        return DriverManager.getConnection(sb.toString());
    }

    public static Connection getConnection() throws Exception {
        return getConnection(Collections.emptyMap());
    }

    public static Connection getConnectionWithTimezone(String zoneId) throws Exception {
        return getConnection(ImmutableMap.of("timezone", zoneId));
    }

    public static Odps getOdps() throws Exception {
        String endpoint = System.getenv("odps_endpoint");
        String project = System.getenv("MAXCOMPUTE_PROJECT");
        String accessId = System.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID");
        String accessKey = System.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET");
        String stsToken = System.getenv("ALIBABA_CLOUD_SECURITY_TOKEN");

        Account account =
            StringUtils.isBlank(stsToken) ? new AliyunAccount(accessId, accessKey)
                                          : new StsAccount(accessId, accessKey, stsToken);
        Odps odps = new Odps(account);
        odps.setDefaultProject(project);
        odps.setEndpoint(endpoint);
        return odps;
    }
}
