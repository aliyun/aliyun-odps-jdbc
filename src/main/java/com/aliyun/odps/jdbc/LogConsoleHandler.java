package com.aliyun.odps.jdbc;


import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;

public class LogConsoleHandler {

  static class LogFormatter extends Formatter {
    @Override
    public String format(LogRecord record) {
      return record.getLoggerName() + " [" + record.getLevel() + "] " + record.getMessage() + "\n";
    }
  }

  private static ConsoleHandler handler;
  public static ConsoleHandler getInstance() {
    if (handler == null) {
      handler = new ConsoleHandler();
      handler.setLevel(Level.ALL);
      handler.setFormatter(new LogFormatter());
    }
    return handler;
  }
}



