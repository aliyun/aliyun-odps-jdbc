package com.aliyun.odps.jdbc;

import java.sql.Date;
import java.sql.Time;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.TimeZone;

import org.junit.Test;

import com.aliyun.odps.jdbc.utils.JdbcColumn;


public class TimeTest {

  public static final Calendar ISO8601_LOCAL_CALENDAR = new Calendar.Builder()
      .setCalendarType("iso8601")
      .setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
      .setLenient(true)
      .build();

  public static void main(String[] args) throws ParseException {

    String timeStr = "0001-01-01 00:00:00";
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    simpleDateFormat.setCalendar(ISO8601_LOCAL_CALENDAR);

    Date date = new Date(simpleDateFormat.parse(timeStr).getTime());
    System.out.println("sql " + date);
    System.out.println("sql long " + date.getTime());

    java.util.Date utilDate = new java.util.Date(simpleDateFormat.parse(timeStr).getTime());
    System.out.println("util " + utilDate);
    System.out.println("util long " + utilDate.getTime());

    DateTimeFormatter
        dateTimeFormatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());
    LocalDate localDate = LocalDate.parse(timeStr, dateTimeFormatter);
    System.out.println("local date " + localDate);
    System.out.println("local date long");

    System.out.println("----" + java.sql.Date.valueOf(localDate));

    LocalDateTime localDateTime = LocalDateTime.parse(timeStr, dateTimeFormatter);
    System.out.println("local dateTime " + localDateTime);
    System.out.println(
        "local dateTime long " + localDateTime.atZone(ZoneId.systemDefault()).toInstant()
            .toEpochMilli());

    System.out.println("----" + java.sql.Time.valueOf(localDateTime.toLocalTime()));

    ZonedDateTime zonedDateTime2 = localDateTime.atZone(ZoneId.systemDefault());
    System.out.println("zone " + zonedDateTime2);
    System.out.println("zone long" + zonedDateTime2.toInstant().toEpochMilli());

    Date date1 = new Date(zonedDateTime2.toInstant().toEpochMilli());
    System.out.println("zone to sql " + date1);
    System.out.println("zone to sql long" + date1.getTime());

    Time time1 = new Time(zonedDateTime2.toInstant().toEpochMilli());
    System.out.println("zone to sql time  " + time1);
    System.out.println("zone to sql time long" + time1.getTime());

    Date
        d =
        new Date(
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("0001-01-01 00:00:00").getTime());
    System.out.println(d);
    System.out.println("=" + d.getTime());

    java.util.Date dd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("0001-01-01 00:00:00");
    System.out.println(dd);
    System.out.println("==" + dd.getTime());

    Instant instant = dd.toInstant();
    // Instant + default time zone.
    ZonedDateTime zonedDateTime = instant.atZone(ZoneId.systemDefault());
    System.out.println(zonedDateTime);
    System.out.println(zonedDateTime.toLocalDate());
    System.out.println(zonedDateTime.toLocalDateTime());
    System.out.println("===" + Date.from(instant));
    System.out.println("====" + instant.toEpochMilli());

    System.out.println(new Date(zonedDateTime.toInstant().toEpochMilli()));
    System.out.println(new Time(zonedDateTime.toInstant().toEpochMilli()));

  }

  @Test
  public void test() throws ParseException {
    DateTimeFormatter
        date2 =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());
    ZonedDateTime
        zonedDateTime =
        LocalDateTime.parse("0001-01-01 00:00:00", date2).atZone(ZoneId.systemDefault());
    System.out.println(zonedDateTime);

    long time = zonedDateTime.toInstant().toEpochMilli();
    java.sql.Date sqlDate = new java.sql.Date(time);
    System.out.println(sqlDate);

    ZonedDateTime
        zonedDateTime1 =
        new java.util.Date(sqlDate.getTime()).toInstant().atZone(ZoneId.systemDefault());
    System.out.println(zonedDateTime1);

    System.out.println(new java.util.Date(sqlDate.getTime()));

    SimpleDateFormat formatter = new SimpleDateFormat(JdbcColumn.ODPS_DATETIME_FORMAT);

    java.sql.Date sqlDate2 = new java.sql.Date(formatter.parse("0001-01-01 00:00:00").getTime());
    System.out.println(String.format("DATETIME\"%s\"", formatter.format(sqlDate)));
    System.out.println(String.format("DATETIME\"%s\"", formatter.format(sqlDate2)));

    String str1 = date2.format((new java.util.Date(sqlDate.getTime())).toInstant());
    String str2 = date2.format((new java.util.Date(sqlDate2.getTime())).toInstant());
    System.out.println(str1);
    System.out.println(str2);

    ZonedDateTime zonedDateTime3 = LocalDateTime.parse(str2, date2).atZone(ZoneId.systemDefault());
    System.out.println(zonedDateTime3);


  }

}
