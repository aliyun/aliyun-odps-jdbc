package com.aliyun.odps.jdbc;

import java.sql.SQLException;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class MaxQARoutingFailureTest {

  @Test
  void preservesBothFailuresForCallerDiagnostics() {
    Exception cause = new Exception("SQL failed");
    Exception routing = new Exception("quota unavailable");
    SQLException result = OdpsConnection.createSQLException("SQL failed", cause, routing);

    assertEquals("SQL failed\nMaxQA routing check failed before fallback: quota unavailable",
                 result.getMessage());
    assertSame(cause, result.getCause());
    assertEquals(1, result.getSuppressed().length);
    assertSame(routing, result.getSuppressed()[0]);
  }

  @Test
  void preservesOriginalExceptionWhenRoutingDidNotFail() {
    Exception cause = new Exception("SQL failed");
    SQLException result = OdpsConnection.createSQLException("SQL failed", cause, null);

    assertEquals("SQL failed", result.getMessage());
    assertSame(cause, result.getCause());
    assertEquals(0, result.getSuppressed().length);
  }

  @Test
  void retainsRoutingExceptionEvenWithoutItsMessage() {
    Exception cause = new Exception("SQL failed");
    for (String message : new String[] {null, ""}) {
      Exception routing = new Exception(message);
      SQLException result = OdpsConnection.createSQLException("SQL failed", cause, routing);

      assertEquals("SQL failed", result.getMessage());
      assertSame(cause, result.getCause());
      assertEquals(1, result.getSuppressed().length);
      assertSame(routing, result.getSuppressed()[0]);
    }
  }

  @Test
  void handlesMissingOuterMessageWithoutDuplicatingCause() {
    Exception routing = new Exception("quota unavailable");
    for (String message : new String[] {null, ""}) {
      SQLException result = OdpsConnection.createSQLException(message, routing, routing);

      assertEquals("MaxQA routing check failed before fallback: quota unavailable",
                   result.getMessage());
      assertSame(routing, result.getCause());
      assertEquals(0, result.getSuppressed().length);
    }
  }
}
