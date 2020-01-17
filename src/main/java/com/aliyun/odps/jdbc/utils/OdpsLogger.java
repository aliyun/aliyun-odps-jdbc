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

  private static final String DEFAULT_OUTPUT_DIR = "/tmp/odps_jdbc";
  private static Map<String, FileHandler> pathToFileHandler = new ConcurrentHashMap<>();

  private Logger odpsLogger;
  private org.slf4j.Logger sl4jLogger;

  /**
   * Constructor
   *
   * @param name           For both odps and sl4j logger, name of the logger
   * @param outputDir      For odps logger, output directory for file handler
   * @param toConsole      For odps logger, output to console or not
   * @param configFilePath For sl4j logger, config file path
   */
  public OdpsLogger(String name, String outputDir, boolean toConsole, String configFilePath) {

    Objects.requireNonNull(name);

    // Init odps logger
    if (outputDir == null) {
      outputDir = getDefaultOutputDir();
    }
    odpsLogger = Logger.getLogger(name);
    try {
      FileHandler fileHandler;
      if (pathToFileHandler.containsKey(outputDir)) {
        fileHandler = pathToFileHandler.get(outputDir);
      } else {

        String outputPattern = Paths.get(outputDir, "odps_jdbc.%g.log").toString();
        fileHandler = new FileHandler(outputPattern,
                                      100 * 1024 * 1024,
                                      50,
                                      true);
        fileHandler.setFormatter(new OdpsFormatter());
        fileHandler.setLevel(Level.WARNING);
      }
      odpsLogger.addHandler(fileHandler);
    } catch (IOException e) {
      // ignore
    }
    if (toConsole) {
      Handler consoleHandler = new ConsoleHandler();
      consoleHandler.setFormatter(new OdpsFormatter());
      consoleHandler.setLevel(Level.WARNING);
      odpsLogger.addHandler(consoleHandler);
    }
    odpsLogger.setLevel(Level.WARNING);

    // Init sl4j logger
    sl4jLogger = LoggerFactory.getLogger(configFilePath, name);
  }

  public synchronized void debug(String msg) {
    odpsLogger.fine(msg);
    sl4jLogger.debug(msg);
  }

  public synchronized void info(String msg) {
    odpsLogger.info(msg);
    sl4jLogger.info(msg);
  }

  public synchronized void warn(String msg) {
    odpsLogger.warning(msg);
    sl4jLogger.warn(msg);
  }

  public synchronized void error(String msg) {
    odpsLogger.severe(msg);
    sl4jLogger.error(msg);
  }

  public synchronized void error(String msg, Throwable e) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    e.printStackTrace(pw);

    odpsLogger.severe(msg);
    odpsLogger.severe(sw.toString());
    sl4jLogger.error(msg, e);
  }

  /**
   * Return the default output path. This method tries to return the dir of source code. If it is
   * not allowed due to security reason, return "/tmp"
   *
   * @return default output path
   */
  public static String getDefaultOutputDir() {
    String outputDir;
    try {
      outputDir = new File(OdpsDriver.class.getProtectionDomain().getCodeSource()
                               .getLocation().toURI()).getParent();
    } catch (Exception e) {
      outputDir = DEFAULT_OUTPUT_DIR;
    }
    return outputDir;
  }
}
