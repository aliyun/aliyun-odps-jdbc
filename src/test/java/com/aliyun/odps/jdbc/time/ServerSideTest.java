package com.aliyun.odps.jdbc.time;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.Date;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.TimeZone;

import org.junit.jupiter.api.Test;

import com.aliyun.odps.Odps;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.jdbc.utils.JdbcTimeUtil;
import com.aliyun.odps.jdbc.utils.TestUtils;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;

import java.sql.Timestamp;
import java.time.*;

public class ServerSideTest {

    private static final String DATE_UTC = "2012-01-01";
    private static final String DATETIME_UTC = "2012-01-01 00:00:00";
    private static final String DATETIME_SHANGHAI = "2012-01-01 08:00:00";
    private static final String TIMESTAMP_UTC = "2012-01-01 00:00:00.000000000";
    private static final String TIMESTAMP_NTZ_LITERAL = "2012-01-01 00:00:00";

    private static final long TIMESTAMP_MILLIS_0 = 1325376000000L;

    private static final long TIMESTAMP_MILLIS_MINUS_8 = 1325347200000L;

    @Test
    public void testDate() throws Exception {
        try (Connection conn = TestUtils.getConnectionWithTimezone(
            "UTC"); Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS odps_jdbc_date_test");
            st.execute("CREATE TABLE odps_jdbc_date_test (k STRING, v DATE) ");
            st.execute("INSERT INTO odps_jdbc_date_test VALUES ('test', DATE'" + DATE_UTC + "')");
        }

        Odps odps = TestUtils.getOdps();
        TableTunnel.DownloadSession
            session =
            odps.tableTunnel().buildDownloadSession(odps.getDefaultProject(), "odps_jdbc_date_test")
                .setSchemaName("default").build();
        TunnelRecordReader reader = session.openRecordReader(0, 1);
        Record record = reader.read();
        Object obj = record.get(1);

        assertEquals(LocalDate.class, obj.getClass());
        assertEquals(DATE_UTC, obj.toString());

        long epochDay = ((LocalDate) obj).toEpochDay();
        System.out.println("epochDay=" + epochDay);

        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
        Date
            date =
            JdbcTimeUtil.epochDayToJdbcDate(epochDay, TimeZone.getTimeZone("Asia/Shanghai"));
        assertEquals(TIMESTAMP_MILLIS_MINUS_8, date.getTime());

        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        date = JdbcTimeUtil.epochDayToJdbcDate(epochDay, TimeZone.getTimeZone("UTC"));
        assertEquals(TIMESTAMP_MILLIS_0, date.getTime());
    }

    @Test
    public void testDatetime() throws Exception {
        try (Connection conn = TestUtils.getConnectionWithTimezone(
            "UTC"); Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS odps_jdbc_datetime_test");
            st.execute("CREATE TABLE odps_jdbc_datetime_test (k STRING, v DATETIME) ");
            st.execute("INSERT INTO odps_jdbc_datetime_test VALUES ('utc', DATETIME'" + DATETIME_UTC
                       + "')");
        }
        try (Connection conn = TestUtils.getConnectionWithTimezone(
            "Asia/Shanghai"); Statement st = conn.createStatement()) {
            st.execute(
                "INSERT INTO odps_jdbc_datetime_test VALUES ('sh', DATETIME'" + DATETIME_SHANGHAI
                + "')");
        }

        Odps odps = TestUtils.getOdps();
        TableTunnel.DownloadSession
            session =
            odps.tableTunnel()
                .buildDownloadSession(odps.getDefaultProject(), "odps_jdbc_datetime_test")
                .setSchemaName("default").build();
        TunnelRecordReader reader = session.openRecordReader(0, 2);
        Record record = reader.read();
        Object obj = record.get(1);

        assertEquals(ZonedDateTime.class, obj.getClass());

        // 解释为 UTC millis
        long utcMillis = ((ZonedDateTime) obj).toInstant().toEpochMilli();
        System.out.println("LocalDateTime(UTC)=" + obj + " → millis=" + utcMillis);
        assertEquals(TIMESTAMP_MILLIS_0, utcMillis);

        Timestamp ts = JdbcTimeUtil.toJdbcTimestamp(utcMillis, 0, TimeZone.getDefault());
        System.out.println("java.sql.Timestamp=" + ts + ", millis=" + ts.getTime());

        record = reader.read();
        obj = record.get(1);
        assertEquals(utcMillis, ((ZonedDateTime) obj).toInstant().toEpochMilli());
    }

    @Test
    public void testTimestamp() throws Exception {
        try (Connection conn = TestUtils.getConnectionWithTimezone(
            "UTC"); Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS odps_jdbc_timestamp_test");
            st.execute("CREATE TABLE odps_jdbc_timestamp_test (k STRING, v TIMESTAMP)");
            st.execute(
                "INSERT INTO odps_jdbc_timestamp_test VALUES ('test', TIMESTAMP'" + TIMESTAMP_UTC
                + "')");
        }
        try (Connection conn = TestUtils.getConnectionWithTimezone(
            "Asia/Shanghai"); Statement st = conn.createStatement()) {
            st.execute(
                "INSERT INTO odps_jdbc_timestamp_test VALUES ('sh', TIMESTAMP'" + DATETIME_SHANGHAI
                + "')");
        }

        Odps odps = TestUtils.getOdps();
        TableTunnel.DownloadSession
            session =
            odps.tableTunnel()
                .buildDownloadSession(odps.getDefaultProject(), "odps_jdbc_timestamp_test")
                .setSchemaName("default").build();
        TunnelRecordReader reader = session.openRecordReader(0, 2);
        Record record = reader.read();
        Object obj = record.get(1);

        assertEquals(Instant.class, obj.getClass());
        System.out.println("Server returned: " + obj);

        long utcMillis = ((Instant) obj).toEpochMilli();
        System.out.println("instant=" + obj + " → millis=" + utcMillis);
        assertEquals(TIMESTAMP_MILLIS_0, utcMillis);

        Timestamp ts = JdbcTimeUtil.toJdbcTimestamp(utcMillis, 0, TimeZone.getDefault());
        System.out.println("java.sql.Timestamp=" + ts + ", millis=" + ts.getTime());

        record = reader.read();
        obj = record.get(1);
        assertEquals(utcMillis, ((Instant) obj).toEpochMilli());
    }

    @Test
    public void testTimestampNtz() throws Exception {
        try (Connection conn = TestUtils.getConnectionWithTimezone(
            "UTC"); Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS odps_jdbc_timestamp_ntz_test");
            st.execute("CREATE TABLE odps_jdbc_timestamp_ntz_test (k STRING, v TIMESTAMP_NTZ)");
            st.execute("INSERT INTO odps_jdbc_timestamp_ntz_test VALUES ('test', TIMESTAMP_NTZ'"
                       + TIMESTAMP_NTZ_LITERAL + "')");
        }

        Odps odps = TestUtils.getOdps();
        TableTunnel.DownloadSession
            session =
            odps.tableTunnel()
                .buildDownloadSession(odps.getDefaultProject(), "odps_jdbc_timestamp_ntz_test")
                .setSchemaName("default").build();
        TunnelRecordReader reader = session.openRecordReader(0, 1);
        Record record = reader.read();
        Object obj = record.get(1);

        assertEquals(LocalDateTime.class, obj.getClass());
        System.out.println("Server returned (NTZ, local literal)=" + obj);

        // 注意：NTZ 类型没有时区，Interpret成serverTimezone（假设是UTC）
        long utcMillis = ((LocalDateTime) obj).toInstant(ZoneOffset.UTC).toEpochMilli();
        assertEquals(TIMESTAMP_MILLIS_0, utcMillis);
        System.out.println("LocalDateTime (interpreted as UTC) millis=" + utcMillis);

        Timestamp ts = JdbcTimeUtil.toJdbcTimestamp(utcMillis, 0, TimeZone.getDefault());
        System.out.println("java.sql.Timestamp (from NTZ)=" + ts + ", millis=" + ts.getTime());
    }
}

