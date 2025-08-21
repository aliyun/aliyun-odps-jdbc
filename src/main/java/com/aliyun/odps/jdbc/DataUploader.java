package com.aliyun.odps.jdbc;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.jdbc.utils.transformer.to.odps.AbstractToOdpsTransformer;
import com.aliyun.odps.jdbc.utils.transformer.to.odps.ToOdpsTransformerFactory;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.utils.StringUtils;

public abstract class DataUploader {

  protected String projectName;
  protected String schemaName;
  protected String tableName;
  protected PartitionSpec partitionSpec;
  protected List<String> specificColumns;
  protected OdpsConnection conn;

  protected TableTunnel tunnel;
  protected Table table;
  protected TableSchema tableSchema;
  protected ArrayRecord reuseRecord;

  public DataUploader(String projectName,
                      String schemaName,
                      String tableName,
                      String partitionSpec,
                      List<String> specificColumns,
                      OdpsConnection conn)
      throws OdpsException, IOException {
    this.projectName = projectName;
    this.schemaName = schemaName;
    this.tableName = tableName;
    if (!StringUtils.isNullOrEmpty(partitionSpec)) {
      this.partitionSpec = new PartitionSpec(partitionSpec);
    }
    this.specificColumns = specificColumns;
    this.conn = conn;

    tunnel = new TableTunnel(conn.getOdps());
    Table table = conn.getOdps().tables().get(projectName, schemaName, tableName);
    if (partitionSpec != null && !table.hasPartition(this.partitionSpec)) {
      table.createPartition(this.partitionSpec);
    }
    setUpSession();

    if (specificColumns == null) {
      this.specificColumns = tableSchema.getColumns().stream().map(Column::getName).collect(Collectors.toList());
    }
  }

  public static DataUploader build(String projectName,
                                   String schemaName,
                                   String tableName,
                                   String partitionSpec,
                                   List<String> specificColumns,
                                   OdpsConnection conn) throws OdpsException, IOException {
    Table table = conn.getOdps().tables().get(projectName, tableName);
    if (table.isTransactional() && table.getPrimaryKey() != null && !table.getPrimaryKey().isEmpty()) {
      return new AcidTableUploader(projectName, schemaName, tableName, partitionSpec, specificColumns, conn);
    } else {
      return new BasicTableUploader(projectName, schemaName, tableName, partitionSpec, specificColumns, conn);
    }
  }

  protected abstract void setUpSession() throws OdpsException, IOException;


  public int[] upload(List<Object[]> batchedRows) throws SQLException {

    int batchedSize = batchedRows.size();
    if (batchedSize == 0) {
      return new int[0];
    }

    conn.log.info(batchedSize + " records are going to be uploaded to table "
                  + projectName + "." + tableName + " in batch");

    int[] updateCounts = new int[batchedSize];
    Arrays.fill(updateCounts, -1);

    try {
      upload(batchedRows, batchedSize, updateCounts);
    } catch (Exception e) {
      throw new SQLException(e.getMessage(), e);
    }

    return updateCounts;
  }

  protected abstract void upload(List<Object[]> batchedRows, int batchedSize, int[] updateCounts)
      throws OdpsException, IOException, SQLException;

  protected void setReusedRecord(Object[] row, TableSchema schema) throws SQLException {
    for (int i = 0; i < specificColumns.size(); i++) {
      String columnName = specificColumns.get(i);
      AbstractToOdpsTransformer transformer = ToOdpsTransformerFactory.getTransformer(
          schema.getColumn(columnName).getTypeInfo().getOdpsType());
      reuseRecord.set(columnName, transformer.transform(row[i], conn.getCharset()));
    }
  }

  public abstract void commit() throws TunnelException, IOException;
}
