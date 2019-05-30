package com.aliyun.odps.jdbc.utils;

import java.io.IOException;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class OdpsLogger {
  private Logger logger = Logger.getLogger(OdpsLogger.class.getName());

  public OdpsLogger(String filePath) throws IOException {
    this(filePath, false);
  }

  public OdpsLogger(String filePath, boolean toConsole) throws IOException {
    Handler fileHandler = new FileHandler(filePath);
    fileHandler.setFormatter(new OdpsFormatter());
    fileHandler.setLevel(Level.ALL);
    logger.addHandler(fileHandler);
    if (toConsole) {
      Handler consoleHandler = new ConsoleHandler();
      consoleHandler.setFormatter(new OdpsFormatter());
      consoleHandler.setLevel(Level.ALL);
      logger.addHandler(consoleHandler);
    }

    logger.setLevel(Level.ALL);
  }

  public void debug(String msg) {
    logger.fine(msg);
  }

  public void info(String msg) {
    logger.info(msg);
  }

  public void error(String msg) {
    logger.severe(msg);
  }


}
