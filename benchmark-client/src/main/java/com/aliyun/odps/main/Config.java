package com.aliyun.odps.main;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import lombok.ToString;

@ToString
public class Config {
    private String endpoint;
    private String delimiter = "\t";

    private Properties configProps = new Properties();

    public static Config getInstance() {
        return Loader.INSTANCE;
    }

    public void load(String configFilePath) throws IOException {
        try (FileReader reader = new FileReader(configFilePath)) {
            configProps.load(reader);
            this.endpoint = configProps.getProperty("end_point");
            this.delimiter = configProps.getProperty("delimiter", "\t");
        }
        String[] keys = new String[] {"end_point", "project_name", "quota_name", "access_id", "access_key"};

        StringBuilder sb = new StringBuilder();
        for (String k : keys) {
            if (configProps.getProperty(k) == null) {
                sb.append(k + " is null;");
            }
        }
        if (sb.length() > 0) {
            throw new IllegalArgumentException(sb.toString());
        }
    }

    public Properties getConfigProps() {
        return configProps;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public String buildJdbcUrl() {
        return "jdbc:odps:" + this.endpoint
            + "?skipCheckIfSelect=true&charset=UTF-8&interactiveMode=true&async=true&disableConnectionSetting=true";
    }

    private static class Loader {
        private static final Config INSTANCE = new Config();
    }

}
