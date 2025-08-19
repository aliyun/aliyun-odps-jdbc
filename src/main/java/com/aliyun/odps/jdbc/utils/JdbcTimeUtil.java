package com.aliyun.odps.jdbc.utils;


import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.TimeZone;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class JdbcTimeUtil {

    public static long getEpochMillis(Object o) {
        if (o instanceof LocalDateTime) {
            return ((LocalDateTime) o).toInstant(ZoneOffset.UTC).toEpochMilli();
        } else if (o instanceof ZonedDateTime) {
            return ((ZonedDateTime) o).toInstant().toEpochMilli();
        } else if (o instanceof Instant) {
            return ((Instant) o).toEpochMilli();
        } else {
            throw new IllegalArgumentException("Unexpected time type: " + o.getClass().getName());
        }
    }

    public static int getNanos(Object o) {
        if (o instanceof LocalDateTime) {
            return ((LocalDateTime) o).toInstant(ZoneOffset.UTC).getNano();
        } else if (o instanceof ZonedDateTime) {
            return ((ZonedDateTime) o).toInstant().getNano();
        } else if (o instanceof Instant) {
            return ((Instant) o).getNano();
        } else {
            throw new IllegalArgumentException("Unexpected time type: " + o.getClass().getName());
        }
    }


    /**
     * 从 epochDay 构造 JDBC Date
     *
     * @param epochDay 自 1970-01-01 (UTC) 起的天数
     * @param serverTz serverTimezone 时区 ID（无 Calendar 时使用）
     * @return java.sql.Date（JDBC 规范行为）
     */
    public static Date epochDayToJdbcDate(long epochDay, TimeZone serverTz) {
        // epochDay → 对应的 UTC Instant（当天 00:00 UTC）
        Instant
            utcStartOfDay =
            LocalDate.ofEpochDay(epochDay).atStartOfDay(ZoneId.of("UTC")).toInstant();

        // 转目标时区的 LocalDate
        LocalDate localDate = utcStartOfDay.atZone(serverTz.toZoneId()).toLocalDate();

        // JDBC Date.valueOf() 构造（会去掉时间部分）
        return Date.valueOf(localDate);
    }

    /**
     * 从 UTC 毫秒构造 JDBC Date
     *
     * @param utcMillis UTC 毫秒（1970 epoch）
     * @param serverTz  serverTimezone 时区 ID（无 Calendar 时使用）
     * @return java.sql.Date（JDBC 规范行为）
     */
    public static Date toJdbcDate(long utcMillis, TimeZone serverTz) {
        LocalDateTime
            ldt =
            LocalDateTime.ofInstant(Instant.ofEpochMilli(utcMillis), serverTz.toZoneId());
        return Date.valueOf(ldt.toLocalDate());
    }

    /**
     * 从 UTC 毫秒构造 JDBC Timestamp
     *
     * @param utcMillis UTC 毫秒
     * @param serverTz  serverTimezone 时区 ID
     * @return java.sql.Timestamp（JDBC 规范行为）
     */
    public static Timestamp toJdbcTimestamp(long utcMillis, int nanos, TimeZone serverTz) {
        LocalDateTime
            ldt =
            LocalDateTime.ofInstant(Instant.ofEpochMilli(utcMillis), serverTz.toZoneId());
        Timestamp timestamp = Timestamp.valueOf(ldt);
        timestamp.setNanos(nanos);
        return timestamp;
    }

    /**
     * 从 UTC 毫秒构造 JDBC Time
     *
     * @param utcMillis UTC 毫秒（自 1970 epoch）
     * @param serverTz  serverTimezone 时区 ID（无 Calendar 时使用）
     * @return java.sql.Time（JDBC 规范行为）
     */
    public static Time toJdbcTime(long utcMillis, TimeZone serverTz) {
        LocalTime
            localTime =
            LocalDateTime.ofInstant(Instant.ofEpochMilli(utcMillis), serverTz.toZoneId())
                .toLocalTime();
        return Time.valueOf(localTime);
    }
}
