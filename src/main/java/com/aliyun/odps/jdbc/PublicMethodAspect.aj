package com.aliyun.odps.jdbc;

import com.aliyun.odps.jdbc.utils.OdpsLogger;
import java.io.IOException;
import java.nio.file.Paths;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.reflect.SourceLocation;

public aspect PublicMethodAspect {
  pointcut PublicMethods() : execution(public * com.aliyun.odps.jdbc.*.*(..));

  private OdpsLogger logger;
  public PublicMethodAspect() throws IOException {
    String jarDir = ClassLoader.getSystemClassLoader().getResource(".").getPath();
    logger = new OdpsLogger(Paths.get(jarDir, "jdbc.log").toString(), true);
  }

  before() : PublicMethods() {
    int lineNumber = getCurrentLineNumber(thisJoinPoint);
    String classname = getCurrentClassname(thisJoinPoint);
    String methodName = getCurrentMethodName(thisJoinPoint);
    String msg = String.format(
        "Enter: [line %d] [%s] [%s]", lineNumber, classname, methodName);
    logger.debug(msg);
  }

  after() : PublicMethods() {
    int lineNumber = getCurrentLineNumber(thisJoinPoint);
    String classname = getCurrentClassname(thisJoinPoint);
    String methodName = getCurrentMethodName(thisJoinPoint);
    String msg = String.format(
        "Leave: [line %d] [%s] [%s]", lineNumber, classname, methodName);
    logger.debug(msg);
  }

  after() throwing(Exception e) : PublicMethods() {
    e.printStackTrace();
  }

  private int getCurrentLineNumber(JoinPoint joinPoint) {
    int line = -1;

    SourceLocation sourceLocation = joinPoint.getSourceLocation();
    if (sourceLocation != null) {
      line = sourceLocation.getLine();
    }
    return line;
  }

  private String getCurrentClassname(JoinPoint joinPoint) {
    return joinPoint.getThis().getClass().getName();
  }

  private String getCurrentMethodName(JoinPoint joinPoint) {
    return joinPoint.getSignature().getName();
  }
}
