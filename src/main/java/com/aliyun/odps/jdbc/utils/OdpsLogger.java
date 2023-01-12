package com.aliyun.odps.jdbc.utils;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.aliyun.odps.jdbc.OdpsDriver;

public class OdpsLogger {

  private static final String DEFAULT_OUTPUT_DIR = "/tmp";
  private static Map<String, FileHandler> pathToFileHandler = new ConcurrentHashMap<>();

  private boolean enableOdpsLogger = false;
  private Logger odpsLogger;
  private org.slf4j.Logger sl4jLogger;
  private String connectionId;
  private boolean toConsole = false;

  /**
   * Constructor
   *
   * @param name             For both odps and sl4j logger, name of the logger
   * @param outputPath       For odps logger, output path for file handler
   * @param toConsole        For odps logger, output to console or not
   * @param enableOdpsLogger For odps logger, enable or not
   * @param configFilePath   For sl4j logger, config file path
   */
  public OdpsLogger(String name,
                    String connectionId,
                    String outputPath,
                    String configFilePath,
                    boolean toConsole,
                    boolean enableOdpsLogger) {

    this.connectionId = connectionId;

    this.enableOdpsLogger = enableOdpsLogger;

    Objects.requireNonNull(name);

    // Init odps logger
    if (outputPath == null) {
      outputPath = getDefaultOutputPath();
    }
    if (enableOdpsLogger) {
      odpsLogger = Logger.getLogger(name);
      odpsLogger.setLevel(Level.ALL);
      if (toConsole) {
        if (!this.toConsole) {
          Handler consoleHandler = new ConsoleHandler();
          consoleHandler.setFormatter(new OdpsFormatter());
          consoleHandler.setLevel(Level.ALL);
          odpsLogger.addHandler(consoleHandler);
          this.toConsole = true;
        }
      }

      try {
        FileHandler fileHandler;
        if (!pathToFileHandler.containsKey(outputPath)) {
          fileHandler = new FileHandler(outputPath, true);
          fileHandler.setFormatter(new OdpsFormatter());
          fileHandler.setLevel(Level.ALL);
          pathToFileHandler.put(outputPath, fileHandler);
          odpsLogger.addHandler(fileHandler);
        }
      } catch (IOException e) {
        // ignore
      }
    }

    // Init sl4j logger
    sl4jLogger = LoggerFactory.getLogger(configFilePath, name);
  }

  public synchronized void debug(String msg) {
    if (enableOdpsLogger) {
      odpsLogger.fine(String.format("[connection-%s] %s", connectionId, msg));
    }
    sl4jLogger.debug(msg);
  }

  public synchronized void info(String msg) {
    if (enableOdpsLogger) {
      odpsLogger.info(String.format("[connection-%s] %s", connectionId, msg));
    }
    sl4jLogger.info(msg);
  }

  public synchronized void warn(String msg) {
    if (enableOdpsLogger) {
      odpsLogger.warning(String.format("[connection-%s] %s", connectionId, msg));
    }
    sl4jLogger.warn(msg);
  }

  public synchronized void error(String msg) {
    if (enableOdpsLogger) {
      odpsLogger.severe(String.format("[connection-%s] %s", connectionId, msg));
    }
    sl4jLogger.error(msg);
  }

  public synchronized void error(String msg, Throwable e) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    e.printStackTrace(pw);
    if (enableOdpsLogger) {
      odpsLogger.severe(String.format("[connection-%s] %s", connectionId, msg));
      odpsLogger.severe(String.format("[connection-%s] %s", connectionId, sw.toString()));
    }
    sl4jLogger.error(msg, e);
  }

  /**
   * Return the default output path. This method tries to return the dir of source code. If it is
   * not allowed due to security reason, return "/tmp"
   *
   * @return default output path
   */
  public static String getDefaultOutputPath() {
    String outputPath;
    try {
      outputPath = new File(OdpsDriver.class.getProtectionDomain().getCodeSource()
                                .getLocation().toURI()).getParent();
    } catch (Exception e) {
      outputPath = DEFAULT_OUTPUT_DIR;
    }
    return Paths.get(outputPath, "jdbc.log").toString();
  }
}
