package com.aliyun.odps.jdbc;

import static java.lang.String.format;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelRecordWriter;
import com.aliyun.odps.utils.StringUtils;

public class BasicTableUploader extends DataUploader {

  private TableTunnel.UploadSession uploadSession;
  private int blocks = 0;

  public BasicTableUploader(String projectName, String schemaName, String tableName,
                            String partitionSpec, List<String> specificColumns, OdpsConnection conn)
      throws OdpsException, IOException {
    super(projectName, schemaName, tableName, partitionSpec, specificColumns, conn);
  }


  public void setUpSession() throws OdpsException {
    String tunnelEndpoint = conn.getTunnelEndpoint();
    if (!StringUtils.isNullOrEmpty(tunnelEndpoint)) {
      tunnel.setEndpoint(tunnelEndpoint);
    }
    if (null != partitionSpec) {
      uploadSession = tunnel.createUploadSession(projectName, schemaName, tableName, partitionSpec, false);
    } else {
      uploadSession = tunnel.createUploadSession(projectName, schemaName, tableName, false);
    }

    conn.log.info("create upload session id=" + uploadSession.getId());
    reuseRecord = (ArrayRecord) uploadSession.newRecord();
    tableSchema = uploadSession.getSchema();
  }


  protected void upload(List<Object[]> batchedRows, int batchedSize, int[] updateCounts)
      throws OdpsException, IOException, SQLException {

    long startTime = System.currentTimeMillis();

    try(TunnelRecordWriter recordWriter =  (TunnelRecordWriter) uploadSession.openRecordWriter(blocks, true)) {
      for (int i = 0; i < batchedSize; i++) {
        Object[] row = batchedRows.get(i);
        setReusedRecord(row, tableSchema);
        recordWriter.write(reuseRecord);
        updateCounts[i] = 1;
      }

      long duration = System.currentTimeMillis() - startTime;
      float megaBytesPerSec = (float) recordWriter.getTotalBytes() / 1024 / 1024 / duration * 1000;
      conn.log.info(format("It took me %d ms to insert %d records [%d], %.2f MiB/s", duration,
                           batchedSize,
                           blocks, megaBytesPerSec));
      blocks += 1;
    }
  }


  public void commit() throws TunnelException, IOException {
    if (uploadSession != null && blocks > 0) {
      Long[] blockList = new Long[blocks];
      conn.log.info("commit session: " + blocks + " blocks");
      for (int i = 0; i < blocks; i++) {
        blockList[i] = Long.valueOf(i);
      }
      uploadSession.commit(blockList);
    }
  }
}
