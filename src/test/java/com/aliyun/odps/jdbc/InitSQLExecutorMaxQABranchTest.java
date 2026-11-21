package com.aliyun.odps.jdbc;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.jdbc.utils.ConnectionResource;
import com.aliyun.odps.sqa.ExecuteMode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Regression for the pre-build dereference of the {@code executor} field inside {@link
 * OdpsConnection#initSQLExecutor}: the MaxQA branch used to evaluate {@code
 * executor.getInstance().getId()} in a log statement, while that field is only assigned by {@code
 * builder.build()} a few lines further down. Every connection that entered the branch therefore
 * died with a bare {@link NullPointerException} inside {@code OdpsConnection.<init>}, before any
 * SQL was submitted.
 *
 * <p>The branch is entered by {@code enableMaxQA || interactiveMode == INTERACTIVE_V2}. Passing
 * {@code interactiveMode=maxqa} in the URL maps to {@code INTERACTIVE_V2} and enters it even when
 * the MaxQA quota lookup returns nothing - which is what makes these three cases deterministic
 * with no credentials, no live service and no MaxQA quota: every URL points the driver at a port
 * that refuses connections.
 *
 * <p>Pairing, measured 2026-11-21 on JDK 1.8.0_502 with the same command line on both sides of the
 * three-line deletion: on the unfixed base {@code #testEnteringTheMaxQABranchFailsAfterExecutorConstructionNotBefore}
 * fails, because the driver throws {@code NullPointerException} (message {@code null}, top driver
 * frame {@code OdpsConnection.initSQLExecutor:428}); on the fixed head the constructor reaches
 * {@code builder.build()} and the failure arrives as {@code SQLException} caused by {@code
 * OdpsException} (connection refused). That difference is the assertion. It deliberately does not
 * claim a successful MaxQA attach - creating a session and reading back a query result still needs
 * a real MaxQA-enabled project, which an offline test cannot substitute for.
 */
public class InitSQLExecutorMaxQABranchTest {

  private static final String DEAD_ENDPOINT = "http://127.0.0.1:1/api";

  /**
   * The three switches skip the constructor's other network reads (project timezone, schema-model
   * flag, tunnel endpoint lookup), so the only thing left that can fail is the path under test.
   * {@code retryTime=0} keeps the refused connection from being retried. Deliberately no
   * {@code connectTimeout}/{@code readTimeout} here: on this driver line that knob is also reused
   * as the retry backoff in seconds, so a millisecond-looking value multiplies the wait (see the
   * report behind {@code interactiveMode} retry handling).
   * defaults to 2.12.4, which has no Jupiter engine - it reaches {@code @Test} methods through the
   * JUnit3/POJO checker and only for methods whose names start with {@code test}. Measured
   * 2026-11-21: with non-{@code test}-prefixed names {@code mvn -o test} reported "Tests run: 0"
   * and BUILD SUCCESS, i.e. the file silently did not execute. The three methods below are
   * therefore named {@code test*} and use no Jupiter lifecycle, so they run under both that old
   * surefire and a current Jupiter provider.
   */
  private static final String OFFLINE_SWITCHES =
      "&timezone=Asia%2FShanghai&odpsNamespaceSchema=false&useInstanceTunnel=false&retryTime=0";

  private static String url(String interactiveMode) {
    return "jdbc:odps:" + DEAD_ENDPOINT + "?project=dummy_project&accessId=dummy_access_id"
        + "&accessKey=dummy_access_key&interactiveMode=" + interactiveMode + OFFLINE_SWITCHES;
  }

  private static Properties credentials() {
    Properties info = new Properties();
    info.setProperty("accessId", "dummy_access_id");
    info.setProperty("accessKey", "dummy_access_key");
    return info;
  }

  /** The keyword that selects MaxQA must land in the branch that used to die. */
  @Test
  public void testMaxqaKeywordSelectsTheInteractiveV2Branch() {
    assertEquals(ExecuteMode.INTERACTIVE_V2,
        new ConnectionResource(url("maxqa"), credentials()).isInteractiveMode());
    // Kept alongside because the report used the historical boolean spelling, which selects
    // INTERACTIVE and reaches the branch only when the quota lookup returns a quota.
    assertEquals(ExecuteMode.INTERACTIVE,
        new ConnectionResource(url("true"), credentials()).isInteractiveMode());
    assertEquals(ExecuteMode.OFFLINE,
        new ConnectionResource(url("false"), credentials()).isInteractiveMode());
  }

  /**
   * Entering the branch must not end in a NullPointerException. Before the deletion it did, with no
   * message, because the log statement dereferenced the field that {@code builder.build()} assigns.
   */
  @Test
  public void testEnteringTheMaxQABranchFailsAfterExecutorConstructionNotBefore() {
    try (Connection ignored = DriverManager.getConnection(url("maxqa"), credentials())) {
      // Reaching this line already proves there is no pre-build dereference left.
    } catch (NullPointerException npe) {
      fail("initSQLExecutor dereferenced the executor before builder.build(): "
          + firstDriverFrame(npe) + " (message=" + npe.getMessage() + ")");
    } catch (SQLException e) {
      assertFalse(hasNullPointerCause(e), "NullPointerException must not appear anywhere in the chain: "
          + firstDriverFrame(e));
      assertTrue(e.getCause() instanceof OdpsException,
          "expected the failure to come from the service layer after the executor is built, got "
              + e.getClass().getName() + " / cause " + e.getCause());
    }
  }

  /** Control: a connection that never enters that branch must be unaffected by the change. */
  @Test
  public void testOfflineModeControlStillConstructs() throws Exception {
    try (Connection c = DriverManager.getConnection(url("false"), credentials())) {
      assertTrue(c.isValid(5), "offline-mode control connection should be usable");
    }
  }

  private static boolean hasNullPointerCause(Throwable t) {
    for (Throwable r = t; r != null; r = r.getCause()) {
      if (r instanceof NullPointerException) {
        return true;
      }
    }
    return false;
  }

  /** Makes a failure readable: the first frame inside the driver. */
  private static String firstDriverFrame(Throwable t) {
    for (StackTraceElement s : t.getStackTrace()) {
      if (s.getClassName().startsWith("com.aliyun.odps.jdbc")) {
        return s.getClassName() + "." + s.getMethodName() + ":" + s.getLineNumber();
      }
    }
    return "n/a";
  }
}
