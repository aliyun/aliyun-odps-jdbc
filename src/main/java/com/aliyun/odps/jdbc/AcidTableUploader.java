package com.aliyun.odps.jdbc;

import static java.lang.String.format;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.impl.UpsertRecord;
import com.aliyun.odps.tunnel.streams.UpsertStream;

public class AcidTableUploader extends DataUploader {

  private TableTunnel.UpsertSession upsertSession;

  public AcidTableUploader(String projectName,
                           String schemaName,
                           String tableName,
                           String partitionSpec,
                           List<String> specificColumns,
                           OdpsConnection conn) throws OdpsException, IOException {
    super(projectName, schemaName, tableName, partitionSpec, specificColumns, conn);
  }

  protected void setUpSession() throws OdpsException, IOException {
    TableTunnel.UpsertSession.Builder builder = tunnel.buildUpsertSession(projectName, tableName);

    if (null != partitionSpec) {
      builder.setPartitionSpec(partitionSpec);
    }
    builder.setSchemaName(schemaName);

    upsertSession = builder.build();
    conn.log.info("create upsert session id=" + upsertSession.getId());
    reuseRecord = (UpsertRecord) upsertSession.newRecord();
    tableSchema = upsertSession.getSchema();
  }

  protected void upload(List<Object[]> data, int batchSize, int[] updateCounts)
      throws OdpsException, IOException, SQLException {
    UpsertStream stream = upsertSession.buildUpsertStream().build();

    for (int i = 0; i < data.size(); i++) {
      Object[] row = data.get(i);
      setReusedRecord(row, tableSchema);
      stream.upsert(reuseRecord);
      updateCounts[i] = 1;
    }

    stream.close();
  }

  public void commit() throws TunnelException, IOException {
    if (upsertSession != null) {
      upsertSession.commit(false);
    }
  }
}
