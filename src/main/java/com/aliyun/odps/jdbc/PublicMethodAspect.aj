package com.aliyun.odps.jdbc;

import com.aliyun.odps.jdbc.utils.OdpsLogger;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.reflect.SourceLocation;

public aspect PublicMethodAspect {
  pointcut Include() : execution(public * com.aliyun.odps.jdbc.Odps*.*(..));

  private OdpsLogger logger;
  public PublicMethodAspect() throws IOException, URISyntaxException {
    String jarDir = new File(PublicMethodAspect.class.getProtectionDomain().getCodeSource()
        .getLocation().toURI()).getParent();
    logger = new OdpsLogger(Paths.get(jarDir, "jdbc.log").toString(), true);
  }

  before() : Include() {
    int lineNumber = getCurrentLineNumber(thisJoinPoint);
    String classname = getCurrentClassname(thisJoinPoint);
    String methodName = getCurrentMethodName(thisJoinPoint);
    String args = getCurrentArguments(thisJoinPoint);
    String msg = String.format(
        "Enter: [line %d] [%s] [%s] [%s]", lineNumber, classname, methodName, args);
    logger.debug(msg);
  }

  after() : Include() {
    int lineNumber = getCurrentLineNumber(thisJoinPoint);
    String classname = getCurrentClassname(thisJoinPoint);
    String methodName = getCurrentMethodName(thisJoinPoint);
    String msg = String.format(
        "Leave: [line %d] [%s] [%s]", lineNumber, classname, methodName);
    logger.debug(msg);
  }

  after() throwing(Exception e) : Include() {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    e.printStackTrace(pw);

    logger.error(sw.toString());
  }

  private int getCurrentLineNumber(JoinPoint joinPoint) {
    try {
      return joinPoint.getSourceLocation().getLine();
    } catch (Exception e) {
      return -1;
    }
  }

  private String getCurrentClassname(JoinPoint joinPoint) {
    try {
      return joinPoint.getThis().getClass().getName();
    } catch (Exception e) {
      return "N/A";
    }
  }

  private String getCurrentMethodName(JoinPoint joinPoint) {
    try {
      return joinPoint.getSignature().getName();
    } catch (Exception e) {
      return "N/A";
    }
  }

  private String formatObject(Object o) {
    // null
    if (o == null) {
      return "null";
    }
    // object[]
    StringBuilder sb = new StringBuilder();
    if (o instanceof Object[]) {
      sb.append("[");
      int l = ((Object[])o).length;
      for (int i = 0; i < l; i++) {
        Object oi = ((Object[])o)[i];
        sb.append(formatObject(oi));
        if (i != l - 1) {
          sb.append(", ");
        }
      }
      sb.append("]");
      return sb.toString();
    }
    // default
    sb.append("'");
    sb.append(o.toString());
    sb.append("'");
    return sb.toString();
  }

  private String getCurrentArguments(JoinPoint joinPoint) {
    try {
      return formatObject(joinPoint.getArgs());
    } catch (Exception e) {
      return "?";
    }
  }
}
