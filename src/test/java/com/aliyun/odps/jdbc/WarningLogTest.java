package com.aliyun.odps.jdbc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import com.aliyun.odps.jdbc.utils.OdpsLogger;
import com.aliyun.odps.jdbc.utils.TestUtils;
import com.google.common.collect.ImmutableMap;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class WarningLogTest {

    Logger logger = (Logger) LoggerFactory.getLogger("ROOT");
    ListAppender<ILoggingEvent> listAppender = new ListAppender<>();

    @BeforeEach
    public void setUp() {
        listAppender.start();
        logger.addAppender(listAppender);
    }

    @AfterEach
    public void tearDown() {
        logger.detachAppender(listAppender);
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
        Assertions.assertTrue(listAppender.list.stream().filter(event -> event.getLevel() == Level.WARN)
                              .anyMatch(event -> event.getFormattedMessage()
                                  .contains(
                                      "The length of sql is too long, it may cause performance issues.")));
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
        Assertions.assertTrue(listAppender.list.stream().filter(event -> event.getLevel() == Level.WARN)
                              .anyMatch(event -> event.getFormattedMessage()
                                  .contains(
                                      "SQL execution time exceeds long job warning threshold.")));
    }

}
