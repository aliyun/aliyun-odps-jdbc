package com.aliyun.odps.jdbc.impl;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Provide utility for casting a ODPS type value to a sql value.
 * The type of input value is consistent with how ODPS represents data:
 *
 * e.g:
 * BigInteger -> java.lang.Long
 * DateTime   -> java.util.Date
 *
 * It is safe to convert a date to a integer or a string to date.
 */
public class TypeUtils {

  public static String DEFFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

  public static final String castToString(Object value) {
    if (value == null) {
      return null;
    }

    return value.toString();
  }

  public static final Boolean castToBoolean(Object value) throws SQLException {
    if (value == null) {
      return null;
    }

    if (value instanceof Boolean) {
      return (Boolean) value;
    }

    if (value instanceof Number) {
      return ((Number) value).intValue() == 1;
    }

    if (value instanceof String) {
      String strVal = (String) value;

      if (strVal.length() == 0) {
        return null;
      }

      if ("true".equalsIgnoreCase(strVal)) {
        return Boolean.TRUE;
      }
      if ("false".equalsIgnoreCase(strVal)) {
        return Boolean.FALSE;
      }

      if ("1".equals(strVal)) {
        return Boolean.TRUE;
      }

      if ("0".equals(strVal)) {
        return Boolean.FALSE;
      }

      if ("null".equals(strVal) || "NULL".equals(strVal)) {
        return null;
      }
    }

    throw new SQLException("can not cast to boolean, value : " + value);
  }

  public static final Byte castToByte(Object value) throws SQLException {
    if (value == null) {
      return null;
    }

    if (value instanceof Number) {
      return ((Number) value).byteValue();
    }

    if (value instanceof String) {
      String strVal = (String) value;
      if (strVal.length() == 0) {
        return null;
      }

      if ("null".equals(strVal) || "NULL".equals(strVal)) {
        return null;
      }

      return Byte.parseByte(strVal);
    }

    throw new SQLException("can not cast to byte, value : " + value);
  }

  public static final Short castToShort(Object value) throws SQLException {
    if (value == null) {
      return null;
    }

    if (value instanceof Number) {
      return ((Number) value).shortValue();
    }

    if (value instanceof String) {
      String strVal = (String) value;

      if (strVal.length() == 0) {
        return null;
      }

      if ("null".equals(strVal) || "NULL".equals(strVal)) {
        return null;
      }

      return Short.parseShort(strVal);
    }

    throw new SQLException("can not cast to short, value : " + value);
  }

  public static final Integer castToInt(Object value) throws SQLException {
    if (value == null) {
      return null;
    }

    if (value instanceof Integer) {
      return (Integer) value;
    }

    if (value instanceof Number) {
      return ((Number) value).intValue();
    }

    if (value instanceof String) {
      String strVal = (String) value;

      if (strVal.length() == 0) {
        return null;
      }

      if ("null".equals(strVal)) {
        return null;
      }

      if ("null".equals(strVal) || "NULL".equals(strVal)) {
        return null;
      }

      return Integer.parseInt(strVal);
    }

    throw new SQLException("can not cast to int, value : " + value);
  }

  public static final Long castToLong(Object value) throws SQLException {
    if (value == null) {
      return null;
    }

    if (value instanceof Number) {
      return ((Number) value).longValue();
    }

    if (value instanceof String) {
      String strVal = (String) value;
      if (strVal.length() == 0) {
        return null;
      }

      if ("null".equals(strVal) || "NULL".equals(strVal)) {
        return null;
      }

      return Long.parseLong(strVal);
    }

    throw new SQLException("can not cast to long, value : " + value);
  }

  public static final BigDecimal castToBigDecimal(Object value) {
    if (value == null) {
      return null;
    }

    if (value instanceof BigDecimal) {
      return (BigDecimal) value;
    }

    if (value instanceof BigInteger) {
      return new BigDecimal((BigInteger) value);
    }

    String strVal = value.toString();
    if (strVal.length() == 0) {
      return null;
    }

    return new BigDecimal(strVal);
  }

  public static final Float castToFloat(Object value) throws SQLException {
    if (value == null) {
      return null;
    }

    if (value instanceof Number) {
      return ((Number) value).floatValue();
    }

    if (value instanceof String) {
      String strVal = value.toString();
      if (strVal.length() == 0) {
        return null;
      }

      if ("null".equals(strVal) || "NULL".equals(strVal)) {
        return null;
      }

      return Float.parseFloat(strVal);
    }

    throw new SQLException("can not cast to float, value : " + value);
  }

  public static final Double castToDouble(Object value) throws SQLException {
    if (value == null) {
      return null;
    }

    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    }

    if (value instanceof String) {
      String strVal = value.toString();
      if (strVal.length() == 0) {
        return null;
      }

      if ("null".equals(strVal) || "NULL".equals(strVal)) {
        return null;
      }

      return Double.parseDouble(strVal);
    }

    throw new SQLException("can not cast to double, value : " + value);
  }

  public static final java.sql.Date castToDate(Object value) throws SQLException {
    if (value == null) {
      return null;
    }

    if (value instanceof java.util.Date) {
      return new java.sql.Date(((java.util.Date) value).getTime());
    }

    long longValue = -1;

    if (value instanceof Number) {
      longValue = ((Number) value).longValue();
      return new java.sql.Date(longValue);
    }

    if (value instanceof String) {
      String strVal = (String) value;

      if (strVal.indexOf('-') != -1) {
        String format;
        if (strVal.length() == DEFFAULT_DATE_FORMAT.length()) {
          format = DEFFAULT_DATE_FORMAT;
        } else if (strVal.length() == 10) {
          format = "yyyy-MM-dd";
        } else if (strVal.length() == "yyyy-MM-dd HH:mm:ss".length()) {
          format = "yyyy-MM-dd HH:mm:ss";
        } else {
          format = "yyyy-MM-dd HH:mm:ss.SSS";
        }

        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        try {
          return new java.sql.Date(dateFormat.parse(strVal).getTime());
        } catch (ParseException e) {
          throw new SQLException("can not cast to Date, value : " + strVal);
        }
      }

      if (strVal.length() == 0) {
        return null;
      }

      longValue = Long.parseLong(strVal);
    }

    if (longValue < 0) {
      throw new SQLException("can not cast to Date, value : " + value);
    }

    return new java.sql.Date(longValue);
  }

  public static final java.sql.Time castToTime(Object value) throws SQLException {
    if (value == null) {
      return null;
    }

    if (value instanceof java.util.Date) {
      return new java.sql.Time(((java.util.Date) value).getTime());
    }

    long longValue = -1;

    if (value instanceof Number) {
      longValue = ((Number) value).longValue();
      return new java.sql.Time(longValue);
    }

    if (value instanceof String) {
      String strVal = (String) value;

      if (strVal.indexOf('-') != -1) {
        String format;
        if (strVal.length() == DEFFAULT_DATE_FORMAT.length()) {
          format = DEFFAULT_DATE_FORMAT;
        } else if (strVal.length() == 10) {
          format = "yyyy-MM-dd";
        } else if (strVal.length() == "yyyy-MM-dd HH:mm:ss".length()) {
          format = "yyyy-MM-dd HH:mm:ss";
        } else {
          format = "yyyy-MM-dd HH:mm:ss.SSS";
        }

        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        try {
          return new java.sql.Time((dateFormat.parse(strVal)).getTime());
        } catch (ParseException e) {
          throw new SQLException("can not cast to Date, value : " + strVal);
        }
      }

      if (strVal.length() == 0) {
        return null;
      }

      longValue = Long.parseLong(strVal);
    }

    if (longValue < 0) {
      throw new SQLException("can not cast to Date, value : " + value);
    }

    return new java.sql.Time(longValue);
  }

  public static final java.sql.Timestamp castToTimestamp(Object value) throws SQLException {
    if (value == null) {
      return null;
    }

    if (value instanceof java.util.Date) {
      return new java.sql.Timestamp(((java.util.Date)value).getTime());
    }

    long longValue = -1;

    if (value instanceof Number) {
      longValue = ((Number) value).longValue();
      return new java.sql.Timestamp(longValue);
    }

    if (value instanceof String) {
      String strVal = (String) value;

      if (strVal.indexOf('-') != -1) {
        String format;
        if (strVal.length() == DEFFAULT_DATE_FORMAT.length()) {
          format = DEFFAULT_DATE_FORMAT;
        } else if (strVal.length() == 10) {
          format = "yyyy-MM-dd";
        } else if (strVal.length() == "yyyy-MM-dd HH:mm:ss".length()) {
          format = "yyyy-MM-dd HH:mm:ss";
        } else {
          format = "yyyy-MM-dd HH:mm:ss.SSS";
        }

        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        try {
          return new java.sql.Timestamp((dateFormat.parse(strVal)).getTime());
        } catch (ParseException e) {
          throw new SQLException("can not cast to Date, value : " + strVal);
        }
      }

      if (strVal.length() == 0) {
        return null;
      }

      longValue = Long.parseLong(strVal);
    }

    if (longValue < 0) {
      throw new SQLException("can not cast to Date, value : " + value);
    }

    return new java.sql.Timestamp(longValue);
  }
}
