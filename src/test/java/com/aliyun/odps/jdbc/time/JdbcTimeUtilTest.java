package com.aliyun.odps.jdbc.time;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Calendar;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.aliyun.odps.jdbc.utils.JdbcTimeUtil;

public class JdbcTimeUtilTest {

    private static final long UTC_MILLIS = 1325376000000L; // 2012-01-01T00:00:00Z

    // Asia/Pyongyang = UTC+9
    // Atlantic/Azores = UTC-1
    @ParameterizedTest
    @CsvSource({
        // jvmTz, serverTz, expectedDateTime
        "UTC, UTC, 2012-01-01T00:00:00",
        "Asia/Shanghai, Asia/Shanghai, 2012-01-01T08:00:00",
        "UTC, Asia/Shanghai, 2012-01-01T00:00:00",
    })
    void testToJdbcTimestamp(String jvmTz, String serverTz, String calendarTz, String expected) {
        TimeZone.setDefault(TimeZone.getTimeZone(jvmTz));
        Timestamp ts = JdbcTimeUtil.toJdbcTimestamp(UTC_MILLIS, 0, TimeZone.getTimeZone(serverTz));
        assertEquals(LocalDateTime.parse(expected), ts.toLocalDateTime());
    }

    @Test
    void testToJdbcDate() {
        // 无 Calendar，serverTz=Asia/Shanghai
        Date date = JdbcTimeUtil.toJdbcDate(UTC_MILLIS, TimeZone.getTimeZone("Asia/Shanghai"));
        assertEquals(Date.valueOf("2012-01-01"), date);

        // 有 Calendar=UTC
        Date date2 = JdbcTimeUtil.toJdbcDate(UTC_MILLIS, TimeZone.getTimeZone("Asia/Shanghai"));
        assertEquals(Date.valueOf("2012-01-01"), date2);

        // 有 Calendar=UTC-1
        Date date3 = JdbcTimeUtil.toJdbcDate(UTC_MILLIS, TimeZone.getTimeZone("Atlantic/Azores"));
        assertEquals(Date.valueOf("2011-12-31"), date3);
    }

    @ParameterizedTest
    @CsvSource({
        // jvmTz, serverTz, calendarTz, expectedLocalTime
        "UTC, UTC, 00:00:00",
        "UTC, Asia/Shanghai, 08:00:00",
    })
    void testToJdbcTime(String jvmTz, String serverTz, String expected) {
        TimeZone.setDefault(TimeZone.getTimeZone(jvmTz));
        Time time = JdbcTimeUtil.toJdbcTime(UTC_MILLIS, TimeZone.getTimeZone(serverTz));
        assertEquals(LocalTime.parse(expected), time.toLocalTime());
    }
}

