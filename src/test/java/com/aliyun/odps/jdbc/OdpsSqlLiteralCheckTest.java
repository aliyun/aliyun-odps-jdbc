package com.aliyun.odps.jdbc;

import com.aliyun.odps.jdbc.utils.OdpsLogger;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Random;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;
import static org.junit.jupiter.api.Assertions.*;

/** Preserve literal-check decisions while preventing recursive regex exhaustion. */
@Timeout(10)
public class OdpsSqlLiteralCheckTest {
  private OdpsPreparedStatement statement() {
    OdpsConnection conn = Mockito.mock(OdpsConnection.class);
    conn.log = new OdpsLogger("literal-check-test", null, null, null, false, false, null);
    Mockito.when(conn.getSqlTaskProperties()).thenReturn(new Properties());
    return new OdpsPreparedStatement(conn, "select ? as v");
  }

  private boolean illegal(OdpsPreparedStatement statement, String value) throws Exception {
    Method method = OdpsPreparedStatement.class.getDeclaredMethod("isIllegal", String.class);
    method.setAccessible(true);
    return (Boolean) method.invoke(statement, value);
  }

  @Test
  public void longUnterminatedMarkersAreAcceptedWithoutStackExhaustion() throws Exception {
    StringBuilder value = new StringBuilder();
    for (int i = 0; i < 100000; i++) value.append("/*x");
    assertFalse(illegal(statement(), value.toString()));
  }

  @Test
  public void longCompleteCommentIsRejectedAsSQLException() throws Exception {
    StringBuilder value = new StringBuilder("/*");
    for (int i = 0; i < 300000; i++) value.append('x');
    value.append("*/");
    Method quote = OdpsPreparedStatement.class.getDeclaredMethod("quoteLiteral", String.class, int.class);
    quote.setAccessible(true);
    InvocationTargetException error = assertThrows(InvocationTargetException.class,
        () -> quote.invoke(statement(), value.toString(), 1));
    assertTrue(error.getCause() instanceof SQLException, error.getCause().toString());
    assertTrue(error.getCause().getMessage().contains("SQL injection check"));
  }

  @Test
  public void shortInputDecisionsMatchThePreviousPredicate() throws Exception {
    Pattern previous = Pattern.compile("(')|(--)|(/\\*(?:.|[\\n\\r])*?\\*/)|(\\b(select|update|and|or|delete|insert|truncate|char|substr|ascii|declare|exec|count|master|into|drop|execute)\\b)", Pattern.CASE_INSENSITIVE);
    String[] parts = {"x", "/*", "*/", "'", "--", "select", "SELECT", "selected", " ", "\n", "\r", "\u0085", "\u2028", "\u2029", "中文", "_", "1"};
    OdpsPreparedStatement ps = statement();
    Random random = new Random(20261019);
    for (int i = 0; i < 3000; i++) {
      StringBuilder value = new StringBuilder();
      for (int j = 0; j < 12; j++) value.append(parts[random.nextInt(parts.length)]);
      String text = value.toString();
      assertEquals(previous.matcher(text).find(), illegal(ps, text), "predicate drift: " + i);
    }
  }
  @Test
  public void wideMalformedBatchFailsWithoutRegexStackExhaustion() throws Exception {
    StringBuilder sql = new StringBuilder("insert into perf_absent values (?");
    for (int i = 1; i < 4096; i++) sql.append(",?");
    OdpsPreparedStatement ps = new OdpsPreparedStatement(statement().getConnection(),
        sql.append(") invalid_suffix").toString());
    for (int i = 1; i <= 4096; i++) ps.setInt(i, 1);
    ps.addBatch();
    SQLException error = assertThrows(SQLException.class, ps::executeBatch);
    assertTrue(error.getMessage().contains("batched statement only support"));
    ps.clearBatch();
  }

  @Test
  public void wideMalformedPartitionBatchFailsWithoutRegexStackExhaustion() throws Exception {
    StringBuilder sql = new StringBuilder("insert into perf_absent partition(p0=1");
    for (int i = 1; i < 4096; i++) sql.append(",p").append(i).append("=1");
    OdpsPreparedStatement ps = new OdpsPreparedStatement(statement().getConnection(),
        sql.append(") values (?) invalid_suffix").toString());
    ps.setInt(1, 1); ps.addBatch();
    SQLException error = assertThrows(SQLException.class, ps::executeBatch);
    assertTrue(error.getMessage().contains("batched statement only support"));
    ps.clearBatch();
  }

}
