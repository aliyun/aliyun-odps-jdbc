package com.aliyun.odps.jdbc.utils;

import java.sql.Timestamp;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class OdpsFormatter extends Formatter {

  @Override
  public synchronized String format(LogRecord record) {
    Timestamp timestamp = new Timestamp(record.getMillis());
    String format = "[%s] [connection-%s] [%s] %s\n";
    return String.format(
        format,
        record.getLevel(),
        record.getLoggerName(),
        timestamp.toString(),
        record.getMessage());
  }
}
