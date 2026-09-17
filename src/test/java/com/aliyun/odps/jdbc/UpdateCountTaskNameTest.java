package com.aliyun.odps.jdbc;

import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.sqa.SQLExecutor;
import java.util.*;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class UpdateCountTaskNameTest {
  private OdpsConnection connection() throws Exception {
    Properties p = new Properties();
    p.setProperty("access_id", "test-id");
    p.setProperty("access_key", "test-key");
    p.setProperty("project_name", "test_project");
    return new OdpsConnection("jdbc:odps:http://127.0.0.1:1?timezone=UTC"
        + "&odpsNamespaceSchema=false&tunnelEndpoint=http://127.0.0.1:1", p);
  }

  private void check(String executorName, String actualName, boolean discoveryFails, int rows)
      throws Exception {
    Instance instance = mock(Instance.class);
    Instance.TaskSummary summary = mock(Instance.TaskSummary.class);
    when(summary.getJsonSummary()).thenReturn("{\"Outputs\":{\"target\":[" + rows + "]}}");
    when(instance.getId()).thenReturn("test-instance");
    when(instance.getTaskSummary(anyString())).thenThrow(new OdpsException("task not found"));
    doReturn(summary).when(instance).getTaskSummary(actualName);
    if (discoveryFails) when(instance.getTaskNames()).thenThrow(new OdpsException("listing unavailable"));
    else when(instance.getTaskNames()).thenReturn(new LinkedHashSet<>(Arrays.asList(actualName)));
    SQLExecutor executor = mock(SQLExecutor.class);
    when(executor.getInstance()).thenReturn(instance);
    when(executor.getTaskName()).thenReturn(executorName);
    when(executor.getExecutionLog()).thenReturn(Collections.emptyList());
    try (OdpsConnection conn = connection(); OdpsStatement statement = conn.createStatement()) {
      statement.sqlExecutor = executor;
      assertEquals(rows, statement.executeUpdate("insert into target select 1"));
      assertEquals(rows, statement.getUpdateCount());
      verify(instance).getTaskSummary(actualName);
      if (discoveryFails) verify(instance, never()).getTaskNames();
    }
  }

  @Test public void usesExecutorTaskName() throws Exception { check("sdk_task", "sdk_task", true, 7); }
  @Test public void fallsBackToInstanceTaskName() throws Exception { check("stale_task", "actual_task", false, 9); }
  @Test public void supportsLegacyTaskName() throws Exception { check(null, OdpsStatement.JDBC_SQL_OFFLINE_TASK_NAME, false, 3); }
  @Test public void preservesZeroAffectedRows() throws Exception { check("sdk_task", "sdk_task", true, 0); }
}
