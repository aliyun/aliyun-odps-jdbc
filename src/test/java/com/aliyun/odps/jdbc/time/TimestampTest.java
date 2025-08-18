package com.aliyun.odps.jdbc.time;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import com.aliyun.odps.jdbc.utils.TestUtils;
import com.google.common.collect.ImmutableMap;

/**
 * 针对固定时区渲染模式（Pinned Timezone Mode）的参数化一致性测试。
 *
 * - serverTimezone 由连接参数指定，为最终渲染的逻辑时区
 * - JVM Default TZ 不影响最终渲染结果（除非显式指定 Calendar）
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TimestampTest {

    // 底层 UTC 存储的测试值
    private static final String DB_LITERAL_UTC = "2012-01-01 00:00:00";

    // 期望值映射：serverTimezone -> LocalDateTime 预期值
    // Asia/Shanghai: UTC+8 => 2012-01-01T08:00
    // UTC: UTC+0 => 2012-01-01T00:00
    static final Map<String, LocalDateTime> EXPECTED_LOCALTIME = ImmutableMap.of(
        "Asia/Shanghai", LocalDateTime.of(2012, 1, 1, 8, 0, 0),
        "UTC", LocalDateTime.of(2012, 1, 1, 0, 0, 0)
    );

    /**
     * 参数结构：JVM时区, serverTimezone（连接参数）, Calendar时区（null表示不传）
     */
    static class TestParam {
        final String jvmTz;
        final String serverTz;
        final String calendarTz;

        TestParam(String jvmTz, String serverTz, String calendarTz) {
            this.jvmTz = jvmTz;
            this.serverTz = serverTz;
            this.calendarTz = calendarTz;
        }

        @Override
        public String toString() {
            return "JVM=" + jvmTz +
                   ", serverTz=" + serverTz +
                   ", Calendar=" + (calendarTz == null ? "null" : calendarTz);
        }
    }

    /**
     * 参数组合：
     * 覆盖 JVM 不同默认时区、Server 时区、Calendar 时区三维组合
     */
    static List<TestParam> params() {
        return Arrays.asList(
            new TestParam("UTC", "Asia/Shanghai", null),
            new TestParam("Asia/Shanghai", "Asia/Shanghai", null),
            new TestParam("UTC", "UTC", null),
            new TestParam("Asia/Shanghai", "UTC", null),
            new TestParam("UTC", "Asia/Shanghai", "UTC"),
            new TestParam("UTC", "UTC", "Asia/Shanghai")
        );
    }

    @BeforeAll
    static void prepareData() throws Exception {
        try (Connection conn = TestUtils.getConnectionWithTimezone("UTC");
             Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS odps_jdbc_timestamp_test");
            st.execute("CREATE TABLE odps_jdbc_timestamp_test (k STRING, v TIMESTAMP)");
            st.execute("INSERT INTO odps_jdbc_timestamp_test VALUES ('test', TIMESTAMP'" + DB_LITERAL_UTC + "')");
        }
    }

    @ParameterizedTest(name = "{index} => {0}")
    @MethodSource("params")
    void testPinnedTimezoneBehavior(TestParam p) throws Exception {
        System.out.println("=== Running: " + p + " ===");
        TimeZone.setDefault(TimeZone.getTimeZone(p.jvmTz));

        // 建立带 serverTimezone 参数的连接
        try (Connection conn = TestUtils.getConnectionWithTimezone(p.serverTz);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT v FROM odps_jdbc_timestamp_test WHERE k='test'")) {
            assertTrue(rs.next(), "No data returned!");

            Calendar cal = (p.calendarTz != null) ? Calendar.getInstance(TimeZone.getTimeZone(p.calendarTz)) : null;

            // 确定当前组合的预期本地时间
            String effectiveTz = (p.calendarTz != null ? p.calendarTz : p.serverTz);
            LocalDateTime expected = EXPECTED_LOCALTIME.get(effectiveTz);

            // 1) getTimestamp
            Timestamp ts = (cal != null) ? rs.getTimestamp(1, cal) : rs.getTimestamp(1);
            LocalDateTime tsLdt = ts.toLocalDateTime();
            assertEquals(expected, tsLdt, failMsg("Timestamp", p, tsLdt));

            // 2) getDate
            Date date = (cal != null) ? rs.getDate(1, cal) : rs.getDate(1);
            LocalDateTime dateLdt = date.toLocalDate().atTime(tsLdt.toLocalTime());
            assertEquals(expected, dateLdt, failMsg("Date", p, dateLdt));

            LocalDateTime dateLdt3 = LocalDateTime.ofInstant(Instant.ofEpochMilli(date.getTime()), ZoneId.of(effectiveTz));
            assertEquals(expected, dateLdt3, failMsg("Date", p, dateLdt3));

            // 3) getTime
            Time time = (cal != null) ? rs.getTime(1, cal) : rs.getTime(1);
            LocalTime localTime = time.toLocalTime();
            assertEquals(expected.toLocalTime(), localTime, failMsg("Time", p, localTime));


            if (cal == null) {
                // 4) getString
                String str = rs.getString(1);
                LocalDateTime
                    parsedFromStr =
                    LocalDateTime.parse(str, java.time.format.DateTimeFormatter.ofPattern(
                        "yyyy-MM-dd HH:mm:ss"));
                assertEquals(expected, parsedFromStr, failMsg("String", p, str));

                // 5) getObject
                Object obj = rs.getObject(1);
                assertEquals(Instant.class, obj.getClass(), failMsg("Instant", p, obj));
                LocalDateTime tsLdt2 = ts.toLocalDateTime();
                assertEquals(expected, tsLdt2, failMsg("Instant", p, tsLdt));

                // 5) getObject(ZonedDateTime)
                obj = rs.getObject(1, ZonedDateTime.class);
                assertEquals(ZonedDateTime.class, obj.getClass(), failMsg("ZonedDateTime", p, obj));
                if (((ZonedDateTime) obj).getZone().getId().equals("UTC")) {
                    assertEquals("2012-01-01T00:00Z[UTC]", obj.toString(), failMsg("ZonedDateTime", p, obj));
                } else {
                    assertEquals("2012-01-01T08:00+08:00[Asia/Shanghai]", obj.toString(), failMsg("ZonedDateTime", p, obj));
                }

                // 6) getObject(LocalDateTime)
                obj = rs.getObject(1, LocalDateTime.class);
                assertEquals(LocalDateTime.class, obj.getClass(), failMsg("LocalDateTime", p, obj));
                LocalDateTime tsLdt3 = (LocalDateTime) obj;
                assertEquals(expected, tsLdt3, failMsg("LocalDateTime", p, tsLdt));
            }
        }
    }

    private static String failMsg(String field, TestParam p, Object actual) {
        return String.format("[FAIL] %s mismatch for %s, got: %s", field, p, actual);
    }
}
