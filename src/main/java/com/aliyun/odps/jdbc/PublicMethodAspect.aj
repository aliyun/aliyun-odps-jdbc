package com.aliyun.odps.jdbc;

import java.io.IOException;
import java.net.URISyntaxException;

import org.aspectj.lang.JoinPoint;

import com.aliyun.odps.jdbc.utils.OdpsLogger;

public aspect PublicMethodAspect {
  pointcut Include(): execution(public * com.aliyun.odps.jdbc.Odps*.*(..));

  private OdpsLogger logger;

  public PublicMethodAspect() throws IOException, URISyntaxException {
    logger = new OdpsLogger(getClass().getName(), null, null, null, false, false);
  }

  before(): Include() {
    int lineNumber = getCurrentLineNumber(thisJoinPoint);
    String classname = getCurrentClassname(thisJoinPoint);
    String methodName = getCurrentMethodName(thisJoinPoint);
    String args = getCurrentArguments(thisJoinPoint).replaceAll("accessKey=\\w+", "accessKey=***");
    String msg = String.format(
        "Enter: [line %d] [%s] [%s] [%s]", lineNumber, classname, methodName, args);
    logger.debug(msg);
  }

  after() returning(Object ret): Include() {
    int lineNumber = getCurrentLineNumber(thisJoinPoint);
    String classname = getCurrentClassname(thisJoinPoint);
    String methodName = getCurrentMethodName(thisJoinPoint);
    String msg = String.format(
        "Leave: [line %d] [%s] [%s] [%s]", lineNumber, classname, methodName, ret);
    logger.debug(msg);
  }

  after() throwing(Exception e): Include() {
    logger.error("exception happened: ", e);
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
      int l = ((Object[]) o).length;
      for (int i = 0; i < l; i++) {
        Object oi = ((Object[]) o)[i];
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
