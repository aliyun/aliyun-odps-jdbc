package com.aliyun.odps.jdbc.time;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
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
 * <p>
 * - serverTimezone 由连接参数指定，为最终渲染的逻辑时区
 * - JVM Default TZ 不影响最终渲染结果（除非显式指定 Calendar）
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class DateTest {

    // 底层 UTC 存储的测试值
    private static final String DB_LITERAL_UTC = "2012-01-01";

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
            st.execute("DROP TABLE IF EXISTS odps_jdbc_date_test");
            st.execute("CREATE TABLE odps_jdbc_date_test (k STRING, v DATE)");
            st.execute(
                "INSERT INTO odps_jdbc_date_test VALUES ('test', DATE'" + DB_LITERAL_UTC + "')");
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
             ResultSet rs = stmt.executeQuery("SELECT v FROM odps_jdbc_date_test WHERE k='test'")) {
            assertTrue(rs.next(), "No data returned!");

            Calendar
                cal =
                (p.calendarTz != null) ? Calendar.getInstance(TimeZone.getTimeZone(p.calendarTz))
                                       : null;

            // 确定当前组合的预期本地时间
            String effectiveTz = (p.calendarTz != null ? p.calendarTz : p.serverTz);
            LocalDateTime expected = EXPECTED_LOCALTIME.get(effectiveTz);

            // 2) getDate
            Date date = (cal != null) ? rs.getDate(1, cal) : rs.getDate(1);
            LocalDateTime dateLdt3 =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(date.getTime()),
                                        ZoneId.of(effectiveTz));
            assertEquals(expected, dateLdt3, failMsg("Date", p, dateLdt3));

            if (cal == null) {
                // 4) getString
                String str = rs.getString(1);
                LocalDate parsedFromStr = LocalDate.parse(str);
                assertEquals(expected.toLocalDate(), parsedFromStr, failMsg("String", p, str));

                // 5) getObject
                Object obj = rs.getObject(1);
                assertEquals(LocalDate.class, obj.getClass(), failMsg("LocalDate", p, obj));
                assertEquals(expected.toLocalDate(), ((LocalDate) obj),
                             failMsg("LocalDate", p, obj));
            }
        }
    }

    private static String failMsg(String field, TestParam p, Object actual) {
        return String.format("[FAIL] %s mismatch for %s, got: %s", field, p, actual);
    }
}
