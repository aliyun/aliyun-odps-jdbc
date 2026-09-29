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
import java.util.Calendar;
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
     * Wall clock this driver shows a JDBC caller for a native (already typed) value.
     *
     * <p>An absolute value -- DATETIME ({@link ZonedDateTime}) and TIMESTAMP ({@link Instant}) --
     * is printed in the session timezone. That is the driver's long-standing display rule:
     * {@code getTimestamp().toString()} reads back as the clock the server printed, and it matches
     * what {@code cast(col as string)} returns on the server.
     *
     * <p>{@link LocalDateTime} is TIMESTAMP_NTZ, which names no timezone at all: the stored clock is
     * the clock the caller must get. Re-anchoring it through the epoch and the session timezone
     * moves the value by the session offset, and at a day boundary it moves the *date*, so the
     * session timezone is deliberately not applied here. A caller's Calendar still is: it is an
     * explicit statement about which zone that clock belongs to, and applying it is what keeps a
     * NTZ column and its own text form reading back the same way.
     *
     * <p>Nanoseconds come from the value itself: a DATETIME keeps millisecond digits, a TIMESTAMP
     * and a TIMESTAMP_NTZ keep nine, including for pre-epoch values, where the fraction is carried
     * as a fraction and never as truncated milliseconds since the epoch.
     */
    public static LocalDateTime nativeWallClock(Object o, TimeZone sessionTz) {
        if (o instanceof LocalDateTime) {
            return (LocalDateTime) o;
        }
        ZoneId display = sessionTz == null ? ZoneId.systemDefault() : sessionTz.toZoneId();
        if (o instanceof ZonedDateTime) {
            return ((ZonedDateTime) o).withZoneSameInstant(display).toLocalDateTime();
        }
        if (o instanceof Instant) {
            return LocalDateTime.ofInstant((Instant) o, display);
        }
        throw new IllegalArgumentException("Unexpected time type: " + o.getClass().getName());
    }

    /**
     * Re-anchor a displayed wall clock the way the text-parsing path already does when the caller
     * hands over a {@link Calendar}: the clock is taken to be local to the Calendar's zone and is
     * then printed in the session zone. A Calendar that agrees with the session zone -- or no
     * Calendar at all -- changes nothing, so a native date/time column gets the same reading a
     * character column with the same clock already gets.
     */
    public static LocalDateTime shiftToCalendar(LocalDateTime wall, TimeZone sessionTz,
                                                TimeZone calendarTz) {
        ZoneId display = sessionTz == null ? ZoneId.systemDefault() : sessionTz.toZoneId();
        return wall.atZone(calendarTz.toZoneId()).withZoneSameInstant(display).toLocalDateTime();
    }

    /**
     * {@code ResultSet#getTimestamp(int)} and {@code getTimestamp(int, Calendar)} for a native
     * value. Pass the Calendar only when it has not already been spent parsing a character value,
     * otherwise the re-anchoring is applied twice.
     *
     * <p>TIMESTAMP_NTZ is deliberately excluded from the session-timezone rendering but not from
     * the Calendar: see {@link #nativeWallClock(Object, TimeZone)}.
     */
    public static Timestamp nativeToJdbcTimestamp(Object o, TimeZone sessionTz, Calendar cal) {
        return Timestamp.valueOf(nativeClock(o, sessionTz, cal));
    }

    /** {@code ResultSet#getDate(int)} and {@code getDate(int, Calendar)} for a native value. */
    public static Date nativeToJdbcDate(Object o, TimeZone sessionTz, Calendar cal) {
        return Date.valueOf(nativeClock(o, sessionTz, cal).toLocalDate());
    }

    /** {@code ResultSet#getTime(int)} and {@code getTime(int, Calendar)} for a native value. */
    public static Time nativeToJdbcTime(Object o, TimeZone sessionTz, Calendar cal) {
        return Time.valueOf(nativeClock(o, sessionTz, cal).toLocalTime());
    }

    private static LocalDateTime nativeClock(Object o, TimeZone sessionTz, Calendar cal) {
        LocalDateTime wall = nativeWallClock(o, sessionTz);
        if (cal != null && cal.getTimeZone() != null) {
            wall = shiftToCalendar(wall, sessionTz, cal.getTimeZone());
        }
        return wall;
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
