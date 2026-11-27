package com.aliyun.odps.jdbc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import com.aliyun.odps.jdbc.utils.TestUtils;
import com.google.common.collect.ImmutableMap;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
@org.junit.jupiter.api.Tag("live-service")
public class WarningLogTest {

    private final Logger logger = Logger.getLogger(OdpsConnection.class.getName());
    private final List<String> warnings = new CopyOnWriteArrayList<>();
    private final Handler handler = new Handler() {
        @Override
        public void publish(LogRecord record) {
            if (record.getLevel() == Level.WARNING) {
                warnings.add(record.getMessage());
            }
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    };

    @BeforeEach
    public void setUp() {
        logger.addHandler(handler);
    }

    @AfterEach
    public void tearDown() {
        logger.removeHandler(handler);
    }

    @Test
    public void testLongSQLLog() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            sb.append(" ");
        }
        String longSql = "select" + sb.toString() + "1;";

        try (Connection connection = TestUtils.getConnection(); Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(
            longSql)) {
            while (resultSet.next()) {
                // do nothing
            }
        }
        Assertions.assertTrue(warnings.stream().anyMatch(message ->
            message.contains("The length of sql is too long, it may cause performance issues.")));
    }

    @Test
    public void testLongJobWarningThreshold() throws Exception {
        // set longJobWarningThreshold=100ms to ensure the warning log is triggered
        try (Connection connection = TestUtils.getConnection(ImmutableMap.of("longJobWarningThreshold", "100"));
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(
            "select 1;")) {
            while (resultSet.next()) {
                // do nothing
            }
        }
        Assertions.assertTrue(warnings.stream().anyMatch(message ->
            message.contains("SQL execution time exceeds long job warning threshold.")));
    }

}
