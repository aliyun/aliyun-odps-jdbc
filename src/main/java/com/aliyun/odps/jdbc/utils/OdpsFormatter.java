package com.aliyun.odps.jdbc.utils;

import java.sql.Timestamp;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class OdpsFormatter extends Formatter {
  private String connectionId = null;

  OdpsFormatter() {
    super();
  }

  OdpsFormatter(String connectionId) {
    super();

    this.connectionId = connectionId;
  }

  @Override
  public synchronized String format(LogRecord record) {
    Timestamp timestamp = new Timestamp(record.getMillis());
    String format = "[%s] [%s] [connection-%s] [%s] %s\n";
    return String.format(
        format,
        record.getLevel(),
        record.getLoggerName(),
        this.connectionId,
        timestamp.toString(),
        record.getMessage());
  }
}
