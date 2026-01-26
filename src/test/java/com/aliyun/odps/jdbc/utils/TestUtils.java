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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.account.StsAccount;
import com.aliyun.odps.utils.StringUtils;
import com.google.common.collect.ImmutableMap;

public class TestUtils {

    private static final Logger log = org.slf4j.LoggerFactory.getLogger(TestUtils.class);
    private static Random random = new Random();
    private static boolean envLoaded = false;

    static {
        loadEnvFile();
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
        return new java.util.Date(
            System.currentTimeMillis() + 86400000 * TestUtils.randomInt(100000));
    }

    /**
     * Load .env file from project root directory and set environment variables
     */
    public static void loadEnvFile() {
        if (envLoaded) {
            return;
        }

        try {
            String projectRoot = System.getProperty("user.dir");
            File envFile = new File(projectRoot, ".env");

            if (envFile.exists() && envFile.isFile()) {
                log.info("Loading .env file from: {}", envFile.getAbsolutePath());
                Map<String, String> envVars = parseEnvFile(envFile);

                for (Map.Entry<String, String> entry : envVars.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    System.setProperty(key, value);
                    log.debug("Set environment variable: {} = {}", key, maskSensitiveValue(key, value));
                }

                envLoaded = true;
                log.info(".env file loaded successfully");
            } else {
                log.info(".env file not found at: {}", envFile.getAbsolutePath());
            }
        } catch (IOException e) {
            log.warn("Failed to load .env file: {}", e.getMessage());
        }
    }

    /**
     * Parse .env file and return key-value pairs
     */
    private static Map<String, String> parseEnvFile(File envFile) throws IOException {
        Map<String, String> envVars = new HashMap<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(envFile))) {
            String line;
            int lineNumber = 0;

            while ((line = reader.readLine()) != null) {
                lineNumber++;

                line = line.trim();

                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }

                int equalSignIndex = line.indexOf('=');
                if (equalSignIndex == -1) {
                    log.warn("Invalid .env file format at line {}: missing '='", lineNumber);
                    continue;
                }

                String key = line.substring(0, equalSignIndex).trim();
                String value = line.substring(equalSignIndex + 1).trim();

                if (!key.isEmpty()) {
                    envVars.put(key, value);
                }
            }
        }

        return envVars;
    }

    /**
     * Get environment variable value, checking both system environment and .env loaded properties
     */
    public static String getEnvValue(String key) {
        String value = System.getProperty(key);
        if (value == null) {
            value = System.getenv(key);
        }
        return value;
    }

    /**
     * Mask sensitive values for logging
     */
    private static String maskSensitiveValue(String key, String value) {
        if (key.toLowerCase().contains("secret") || key.toLowerCase().contains("key")
            || key.toLowerCase().contains("token") || key.toLowerCase().contains("password")) {
            return "***";
        }
        return value;
    }

    public static Connection getConnection(Map<String, String> properties) throws Exception {

        Class.forName("com.aliyun.odps.jdbc.OdpsDriver");

        String endpoint = getEnvValue("MAXCOMPUTE_ENDPOINT");
        if (StringUtils.isBlank(endpoint)) {
            endpoint = getEnvValue("odps_endpoint");
        }
        String project = getEnvValue("MAXCOMPUTE_PROJECT");
        String accessId = getEnvValue("ALIBABA_CLOUD_ACCESS_KEY_ID");
        String accessKey = getEnvValue("ALIBABA_CLOUD_ACCESS_KEY_SECRET");
        String stsToken = getEnvValue("ALIBABA_CLOUD_SECURITY_TOKEN");

        String url = String.format("jdbc:odps:%s?project=%s&accessId=%s&accessKey=%s",
                                   endpoint, project, accessId, accessKey);
        if (!StringUtils.isBlank(stsToken)) {
            stsToken = stsToken.replace("  ", " ");
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
        String endpoint = getEnvValue("MAXCOMPUTE_ENDPOINT");
        if (StringUtils.isBlank(endpoint)) {
            endpoint = getEnvValue("odps_endpoint");
        }
        String project = getEnvValue("MAXCOMPUTE_PROJECT");
        String accessId = getEnvValue("ALIBABA_CLOUD_ACCESS_KEY_ID");
        String accessKey = getEnvValue("ALIBABA_CLOUD_ACCESS_KEY_SECRET");
        String stsToken = getEnvValue("ALIBABA_CLOUD_SECURITY_TOKEN");
        if (!StringUtils.isBlank(stsToken)) {
            stsToken = stsToken.replace("  ", " ");
        }
        Account account =
            StringUtils.isBlank(stsToken) ? new AliyunAccount(accessId, accessKey)
                                          : new StsAccount(accessId, accessKey, stsToken);
        Odps odps = new Odps(account);
        odps.setDefaultProject(project);
        odps.setEndpoint(endpoint);
        return odps;
    }
}
