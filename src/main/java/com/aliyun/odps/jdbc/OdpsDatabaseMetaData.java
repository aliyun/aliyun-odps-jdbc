/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */

package com.aliyun.odps.jdbc;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import com.aliyun.odps.Column;
import com.aliyun.odps.Function;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;

import com.aliyun.odps.Table;

import com.aliyun.odps.account.AliyunAccount;

public class OdpsDatabaseMetaData extends WrapperAdapter implements DatabaseMetaData {

  private final Logger log;
  private static final String PRODUCT_NAME = "ODPS";
  private static final String PRODUCT_VERSION = "v1.1";
  private static final String DRIVER_NAME = "ODPS";
  private static final int DRIVER_MAJOR_VERSION = 1;
  private static final int DRIVER_MINOR_VERSION = 1;

  private static final String SCHEMA_TERM = "project";
  private static final String CATALOG_TERM = "endpoint";
  private static final String PROCEDURE_TERM = "UDF";

  private OdpsConnection conn;


  OdpsDatabaseMetaData(OdpsConnection conn) {
    this.conn = conn;
    this.log = conn.log;
  }

  @Override
  public boolean allProceduresAreCallable() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean allTablesAreSelectable() throws SQLException {
    return true;
  }

  @Override
  public String getURL() throws SQLException {
    return conn.getOdps().getEndpoint();
  }

  @Override
  public String getUserName() throws SQLException {
    AliyunAccount account = (AliyunAccount) conn.getOdps().getAccount();
    return account.getAccessId();
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean nullsAreSortedHigh() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean nullsAreSortedLow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean nullsAreSortedAtStart() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean nullsAreSortedAtEnd() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getDatabaseProductName() throws SQLException {
    return PRODUCT_NAME;
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException {
    return PRODUCT_VERSION;
  }

  @Override
  public String getDriverName() throws SQLException {
    return DRIVER_NAME;
  }

  @Override
  public String getDriverVersion() throws SQLException {
    return DRIVER_MAJOR_VERSION + "." + DRIVER_MINOR_VERSION;
  }

  @Override
  public int getDriverMajorVersion() {
    return DRIVER_MAJOR_VERSION;
  }

  @Override
  public int getDriverMinorVersion() {
    return DRIVER_MINOR_VERSION;
  }

  @Override
  public boolean usesLocalFiles() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean usesLocalFilePerTable() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean storesMixedCaseIdentifiers() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getIdentifierQuoteString() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getSQLKeywords() throws SQLException {
    return "overwrite ";
  }

  @Override
  public String getNumericFunctions() throws SQLException {
    return " ";
  }

  @Override
  public String getStringFunctions() throws SQLException {
    return " ";
  }

  @Override
  public String getSystemFunctions() throws SQLException {
    return " ";
  }

  @Override
  public String getTimeDateFunctions() throws SQLException {
    return "  ";
  }

  @Override
  public String getSearchStringEscape() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getExtraNameCharacters() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsColumnAliasing() throws SQLException {
    return true;
  }

  @Override
  public boolean nullPlusNonNullIsNull() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsConvert() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsConvert(int fromType, int toType) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsTableCorrelationNames() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsExpressionsInOrderBy() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsOrderByUnrelated() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsGroupBy() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsGroupByUnrelated() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsLikeEscapeClause() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsMultipleResultSets() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMultipleTransactions() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsNonNullableColumns() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMinimumSQLGrammar() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsCoreSQLGrammar() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsExtendedSQLGrammar() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsANSI92FullSQL() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsOuterJoins() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsFullOuterJoins() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsLimitedOuterJoins() throws SQLException {
    return true;
  }

  @Override
  public String getSchemaTerm() throws SQLException {
    return SCHEMA_TERM;
  }

  @Override
  public String getProcedureTerm() throws SQLException {
    return PROCEDURE_TERM;
  }

  @Override
  public String getCatalogTerm() throws SQLException {
    return CATALOG_TERM;
  }

  @Override
  public boolean isCatalogAtStart() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getCatalogSeparator() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsSchemasInDataManipulation() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsPositionedDelete() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsPositionedUpdate() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsSelectForUpdate() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsStoredProcedures() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInComparisons() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsSubqueriesInExists() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsSubqueriesInIns() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsCorrelatedSubqueries() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsUnion() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsUnionAll() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxBinaryLiteralLength() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxCharLiteralLength() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxColumnNameLength() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxColumnsInGroupBy() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxColumnsInIndex() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxColumnsInOrderBy() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxColumnsInSelect() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxColumnsInTable() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxConnections() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxCursorNameLength() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxIndexLength() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxSchemaNameLength() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxProcedureNameLength() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxCatalogNameLength() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxRowSize() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxStatementLength() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxStatements() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxTableNameLength() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxTablesInSelect() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxUserNameLength() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getDefaultTransactionIsolation() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsTransactions() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getProcedures(String catalog, String schemaPattern,
                                 String procedureNamePattern) throws SQLException {

    // Return an empty result set
    OdpsResultSetMetaData meta = new OdpsResultSetMetaData(
        Arrays.asList("PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "RESERVERD",
                      "RESERVERD", "RESERVERD", "REMARKS", "PROCEDURE_TYPE", "SPECIFIC_NAME"),
        Arrays.asList(OdpsType.STRING, OdpsType.STRING, OdpsType.STRING, OdpsType.STRING,
                      OdpsType.STRING, OdpsType.STRING, OdpsType.STRING, OdpsType.BIGINT,
                      OdpsType.STRING));

    return new OdpsStaticResultSet(meta);
  }

  @Override
  public ResultSet getProcedureColumns(String catalog, String schemaPattern,
                                       String procedureNamePattern, String columnNamePattern)
      throws SQLException {
    // Return an empty result set
    OdpsResultSetMetaData meta = new OdpsResultSetMetaData(
        Arrays.asList("STUPID_PLACEHOLDERS", "USELESS_PLACEHOLDER"),
        Arrays.asList(OdpsType.STRING, OdpsType.STRING));

    return new OdpsStaticResultSet(meta);
  }

  @Override
  public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern,
                             String[] types) throws SQLException {

    long begin = System.currentTimeMillis();

    List<Object[]> rows = new ArrayList<Object[]>();

    for (Table t : conn.getOdps().tables()) {

      String tableName = t.getName();
      if (tableNamePattern != null) {
        if (!Utils.matchPattern(tableName, tableNamePattern)) {
          continue;
        }
      }

      String tableType = t.isVirtualView() ? "VIEW" : "TABLE";
      if (types != null && types.length != 0) {
        if (!Arrays.asList(types).contains(tableType)) {
          continue;
        }
      }

      Object[] rowVals = {null, t.getProject(), tableName, tableType, t.getComment(),
                          null, null, null, null, "USER"};
      rows.add(rowVals);
    }

    long end = System.currentTimeMillis();
    log.fine("It took me " + (end - begin) + " ms to get " + rows.size() + " Tables");

    OdpsResultSetMetaData meta = new OdpsResultSetMetaData(
        Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT",
                      "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION"),
        Arrays.asList(OdpsType.STRING, OdpsType.STRING, OdpsType.STRING, OdpsType.STRING,
                      OdpsType.STRING, OdpsType.STRING, OdpsType.STRING, OdpsType.STRING,
                      OdpsType.STRING, OdpsType.STRING));

    return new OdpsStaticResultSet(meta, rows.iterator());
  }

  // TODO
  @Override
  public ResultSet getSchemas() throws SQLException {
    // Return an empty result set
    OdpsResultSetMetaData meta = new OdpsResultSetMetaData(
        Arrays.asList("STUPID_PLACEHOLDERS", "USELESS_PLACEHOLDER"),
        Arrays.asList(OdpsType.STRING, OdpsType.STRING));

    return new OdpsStaticResultSet(meta);
  }

  // TODO
  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern)
      throws SQLException {
    // Return an empty result set
    OdpsResultSetMetaData meta = new OdpsResultSetMetaData(
        Arrays.asList("STUPID_PLACEHOLDERS", "USELESS_PLACEHOLDER"),
        Arrays.asList(OdpsType.STRING, OdpsType.STRING));

    return new OdpsStaticResultSet(meta);
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    // Return an empty result set
    OdpsResultSetMetaData meta = new OdpsResultSetMetaData(
        Arrays.asList("STUPID_PLACEHOLDERS", "USELESS_PLACEHOLDER"),
        Arrays.asList(OdpsType.STRING, OdpsType.STRING));

    return new OdpsStaticResultSet(meta);
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    // Return an empty result set
    OdpsResultSetMetaData meta = new OdpsResultSetMetaData(
        Arrays.asList("STUPID_PLACEHOLDERS", "USELESS_PLACEHOLDER"),
        Arrays.asList(OdpsType.STRING, OdpsType.STRING));

    return new OdpsStaticResultSet(meta);
  }

  @Override
  public ResultSet getColumns(String catalog, String schemaPattern,
                              String tableNamePattern, String columnNamePattern)
      throws SQLException {

    long begin = System.currentTimeMillis();

    List<Object[]> rows = new ArrayList<Object[]>();
    try {
      Table table = conn.getOdps().tables().get(tableNamePattern);
      table.reload();
      // Read column information from table schema
      List<Column> columns = table.getSchema().getColumns();
      for (int i = 0; i < columns.size(); i++) {
        Column col = columns.get(i);
        JdbcColumn jdbcCol = new JdbcColumn(col.getName(), tableNamePattern,
                                            null, col.getType(),
                                            col.getComment(), i + 1);
        Object[] rowVals = {
            null, jdbcCol.getTableSchema(), jdbcCol.getTableName(), jdbcCol.getColumnName(),
            (long) jdbcCol.getType(), jdbcCol.getTypeName(), null, null,
            (long) jdbcCol.getDecimalDigits(),
            (long) jdbcCol.getNumPercRaidx(), (long) jdbcCol.getIsNullable(), jdbcCol.getComment(),
            null, null, null, null, (long) jdbcCol.getOrdinalPos(), jdbcCol.getIsNullableString(),
            null, null, null, null};

        rows.add(rowVals);
      }
    } catch (OdpsException e) {
      throw new SQLException(e);
    }

    long end = System.currentTimeMillis();
    log.fine("It took me " + (end - begin) + " ms to get " + rows.size() + " columns");

    // Build result set meta data
    OdpsResultSetMetaData
        meta =
        new OdpsResultSetMetaData(
            Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME",
                          "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH",
                          "DECIMAL_DIGITS", "NUM_PERC_RADIX", "NULLABLE", "REMARKS",
                          "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH",
                          "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA",
                          "SCOPE_TABLE", "SOURCE_DATA_TYPE"),
            Arrays.asList(OdpsType.STRING, OdpsType.STRING, OdpsType.STRING, OdpsType.STRING,
                          OdpsType.BIGINT, OdpsType.STRING, OdpsType.BIGINT, OdpsType.BIGINT,
                          OdpsType.BIGINT, OdpsType.BIGINT, OdpsType.BIGINT, OdpsType.STRING,
                          OdpsType.STRING, OdpsType.BIGINT, OdpsType.BIGINT, OdpsType.BIGINT,
                          OdpsType.BIGINT, OdpsType.STRING, OdpsType.STRING, OdpsType.STRING,
                          OdpsType.STRING, OdpsType.BIGINT));

    return new OdpsStaticResultSet(meta, rows.iterator());
  }

  @Override
  public ResultSet getColumnPrivileges(String catalog, String schema, String table,
                                       String columnNamePattern) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getTablePrivileges(String catalog, String schemaPattern,
                                      String tableNamePattern) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope,
                                        boolean nullable) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getVersionColumns(String catalog, String schema, String table)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getPrimaryKeys(String catalog, String schema, String table)
      throws SQLException {

    // Return an empty result set
    OdpsResultSetMetaData meta = new OdpsResultSetMetaData(
        Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ",
                      "PK_NAME"),
        Arrays.asList(OdpsType.STRING, OdpsType.STRING, OdpsType.STRING, OdpsType.STRING,
                      OdpsType.BIGINT, OdpsType.STRING));

    return new OdpsStaticResultSet(meta);
  }

  @Override
  public ResultSet getImportedKeys(String catalog, String schema, String table)
      throws SQLException {
    // Return an empty result set
    OdpsResultSetMetaData meta = new OdpsResultSetMetaData(
        Arrays
            .asList("PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT",
                    "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE",
                    "DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY"),
        Arrays.asList(OdpsType.STRING, OdpsType.STRING, OdpsType.STRING, OdpsType.STRING,
                      OdpsType.STRING, OdpsType.STRING, OdpsType.STRING, OdpsType.STRING,
                      OdpsType.BIGINT, OdpsType.BIGINT, OdpsType.BIGINT, OdpsType.STRING,
                      OdpsType.STRING, OdpsType.STRING));

    return new OdpsStaticResultSet(meta);
  }

  @Override
  public ResultSet getExportedKeys(String catalog, String schema, String table)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getCrossReference(String parentCatalog, String parentSchema,
                                     String parentTable, String foreignCatalog,
                                     String foreignSchema, String foreignTable)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getTypeInfo() throws SQLException {
    // Return an empty result set
    OdpsResultSetMetaData meta = new OdpsResultSetMetaData(
        Arrays.asList("STUPID_PLACEHOLDERS", "USELESS_PLACEHOLDER"),
        Arrays.asList(OdpsType.STRING, OdpsType.STRING));

    return new OdpsStaticResultSet(meta);
  }

  @Override
  public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique,
                                boolean approximate) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsResultSetType(int type) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsResultSetConcurrency(int type, int concurrency)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean ownUpdatesAreVisible(int type) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean ownDeletesAreVisible(int type) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean ownInsertsAreVisible(int type) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean othersUpdatesAreVisible(int type) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean othersDeletesAreVisible(int type) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean othersInsertsAreVisible(int type) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean updatesAreDetected(int type) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean deletesAreDetected(int type) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean insertsAreDetected(int type) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsBatchUpdates() throws SQLException {
    return false;
  }

  @Override
  public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern,
                           int[] types) throws SQLException {

    // Return an empty result set
    OdpsResultSetMetaData meta = new OdpsResultSetMetaData(
        Arrays.asList("TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "CLASS_NAME", "DATA_TYPE",
                      "REMARKS", "BASE_TYPE"),
        Arrays.asList(OdpsType.STRING, OdpsType.STRING, OdpsType.STRING, OdpsType.STRING,
                      OdpsType.BIGINT, OdpsType.STRING, OdpsType.BIGINT));

    return new OdpsStaticResultSet(meta);
  }

  @Override
  public OdpsConnection getConnection() throws SQLException {
    return conn;
  }

  @Override
  public boolean supportsSavepoints() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsNamedParameters() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsMultipleOpenResults() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsGetGeneratedKeys() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
      throws SQLException {
    return null;
  }

  @Override
  public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    return null;
  }

  @Override
  public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
                                 String attributeNamePattern) throws SQLException {
    return null;
  }

  @Override
  public boolean supportsResultSetHoldability(int holdability) throws SQLException {
    return false;
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getDatabaseMajorVersion() throws SQLException {
    return 0;
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException {
    return 0;
  }

  @Override
  public int getJDBCMajorVersion() throws SQLException {
    return 0;
  }

  @Override
  public int getJDBCMinorVersion() throws SQLException {
    return 0;
  }

  @Override
  public int getSQLStateType() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean locatorsUpdateCopy() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsStatementPooling() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public RowIdLifetime getRowIdLifetime() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getClientInfoProperties() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getFunctions(String catalog, String schemaPattern,
                                String functionNamePattern) throws SQLException {

    long begin = System.currentTimeMillis();

    List<Object[]> rows = new ArrayList<Object[]>();
    for (Function f : conn.getOdps().functions()) {
      Object[] rowVals = {null, null, f.getName(), 0, (long) functionResultUnknown, null};
      rows.add(rowVals);
    }

    long end = System.currentTimeMillis();
    log.fine("It took me " + (end - begin) + " ms to get " + rows.size() + " functions");

    OdpsResultSetMetaData meta = new OdpsResultSetMetaData(
        Arrays.asList("FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "REMARKS",
                      "FUNCTION_TYPE", "SPECIFIC_NAME"),
        Arrays.asList(OdpsType.STRING, OdpsType.STRING, OdpsType.STRING, OdpsType.STRING,
                      OdpsType.BIGINT, OdpsType.STRING));

    return new OdpsStaticResultSet(meta, rows.iterator());
  }

  @Override
  public ResultSet getFunctionColumns(String catalog, String schemaPattern,
                                      String functionNamePattern, String columnNamePattern)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
                                    String columnNamePattern) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  public boolean generatedKeyAlwaysReturned() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }
}
