package com.aliyun.odps.jdbc.time;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.*;

import com.aliyun.odps.jdbc.utils.JdbcTimeUtil;

public class JdbcTimeUtilTest {

    private static final long UTC_MILLIS = 1325376000000L; // 2012-01-01T00:00:00Z


    @AfterAll
    public static void tearDown() {
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
    }

    // Asia/Pyongyang = UTC+9
    // Atlantic/Azores = UTC-1
    @ParameterizedTest
    @CsvSource({
        // jvmTz, serverTz, expectedDateTime
        "UTC, UTC, 2012-01-01T00:00:00",
        "Asia/Shanghai, Asia/Shanghai, 2012-01-01T08:00:00",
        "UTC, Asia/Shanghai, 2012-01-01T08:00:00",
    })
    void testToJdbcTimestamp(String jvmTz, String serverTz, String expected) {
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

    @Test
    void testGetEpochMillis() {
        // Test LocalDateTime
        LocalDateTime localDateTime = LocalDateTime.of(2012, 1, 1, 0, 0, 0);
        long expectedMillis = localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
        assertEquals(expectedMillis, JdbcTimeUtil.getEpochMillis(localDateTime));

        // Test ZonedDateTime
        ZonedDateTime zonedDateTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneId.of("UTC"));
        long expectedZonedMillis = zonedDateTime.toInstant().toEpochMilli();
        assertEquals(expectedZonedMillis, JdbcTimeUtil.getEpochMillis(zonedDateTime));

        // Test Instant
        Instant instant = Instant.ofEpochMilli(UTC_MILLIS);
        assertEquals(UTC_MILLIS, JdbcTimeUtil.getEpochMillis(instant));

        // Test unsupported type
        assertThrows(IllegalArgumentException.class, () -> {
            JdbcTimeUtil.getEpochMillis("invalid");
        });
    }

    @Test
    void testGetNanos() {
        // Test LocalDateTime
        LocalDateTime localDateTime = LocalDateTime.of(2012, 1, 1, 0, 0, 0, 123456789);
        int expectedNanos = localDateTime.toInstant(ZoneOffset.UTC).getNano();
        assertEquals(expectedNanos, JdbcTimeUtil.getNanos(localDateTime));

        // Test ZonedDateTime
        ZonedDateTime zonedDateTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 987654321, ZoneId.of("UTC"));
        int expectedZonedNanos = zonedDateTime.toInstant().getNano();
        assertEquals(expectedZonedNanos, JdbcTimeUtil.getNanos(zonedDateTime));

        // Test Instant
        Instant instant = Instant.ofEpochSecond(UTC_MILLIS / 1000, 555555555);
        assertEquals(555555555, JdbcTimeUtil.getNanos(instant));

        // Test unsupported type
        assertThrows(IllegalArgumentException.class, () -> {
            JdbcTimeUtil.getNanos("invalid");
        });
    }

    @Test
    void testEpochDayToJdbcDate() {
        // Test epoch day conversion
        long epochDay = 15340; // 2012-01-01
        Date date = JdbcTimeUtil.epochDayToJdbcDate(epochDay, TimeZone.getTimeZone("UTC"));
        assertEquals(Date.valueOf("2012-01-01"), date);

        // Test with different timezone
        Date date2 = JdbcTimeUtil.epochDayToJdbcDate(epochDay, TimeZone.getTimeZone("Asia/Shanghai"));
        assertEquals(Date.valueOf("2012-01-01"), date2);
    }

    @Test
    void testToJdbcTimestampWithNanos() {
        // Test timestamp with nanos
        Timestamp ts = JdbcTimeUtil.toJdbcTimestamp(UTC_MILLIS, 123456789, TimeZone.getTimeZone("UTC"));
        assertEquals(LocalDateTime.of(2012, 1, 1, 0, 0, 0, 123456789), ts.toLocalDateTime());
        assertEquals(123456789, ts.getNanos());
    }
}

