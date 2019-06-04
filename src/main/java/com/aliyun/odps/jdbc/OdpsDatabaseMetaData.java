/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
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

import org.slf4j.Logger;

import com.aliyun.odps.Column;
import com.aliyun.odps.Function;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Table;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.jdbc.utils.JdbcColumn;
import com.aliyun.odps.jdbc.utils.Utils;
import com.aliyun.odps.type.TypeInfoFactory;
import com.aliyun.odps.utils.StringUtils;

public class OdpsDatabaseMetaData extends WrapperAdapter implements DatabaseMetaData {

  private final Logger log;
  private static final String PRODUCT_NAME = "MaxCompute/ODPS";
  private static final String DRIVER_NAME = "odps-jdbc";

  private static final String SCHEMA_TERM = "project";
  private static final String CATALOG_TERM = "endpoint";
  private static final String PROCEDURE_TERM = "UDF";

  private static final int TABLE_NAME_LENGTH = 128;
  private static final String DEFAULT_ODPS_CATALOG = "";

  private OdpsConnection conn;


  OdpsDatabaseMetaData(OdpsConnection conn) {
    this.conn = conn;
    this.log = conn.log;
  }

  @Override
  public boolean allProceduresAreCallable() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
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
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean nullsAreSortedHigh() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean nullsAreSortedLow() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean nullsAreSortedAtStart() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean nullsAreSortedAtEnd() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getDatabaseProductName() throws SQLException {
    return PRODUCT_NAME;
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException {
    return Utils.retrieveVersion("sdk.version");
  }

  @Override
  public String getDriverName() throws SQLException {
    return DRIVER_NAME;
  }

  @Override
  public String getDriverVersion() throws SQLException {
    return Utils.retrieveVersion("driver.version");
  }

  @Override
  public int getDriverMajorVersion() {
    try {
      return Integer.parseInt(Utils.retrieveVersion("driver.version").split("\\.")[0]);
    } catch (Exception e) {
      e.printStackTrace();
      return 1;
    }
  }

  @Override
  public int getDriverMinorVersion() {
    try {
      return Integer.parseInt(Utils.retrieveVersion("driver.version").split("\\.")[1]);
    } catch (Exception e) {
      e.printStackTrace();
      return 0;
    }
  }

  @Override
  public boolean usesLocalFiles() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean usesLocalFilePerTable() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean storesMixedCaseIdentifiers() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getIdentifierQuoteString() throws SQLException {
    return "`";
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
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getExtraNameCharacters() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
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
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsConvert() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsConvert(int fromType, int toType) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsTableCorrelationNames() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsExpressionsInOrderBy() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsOrderByUnrelated() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsGroupBy() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsGroupByUnrelated() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsLikeEscapeClause() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsMultipleResultSets() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMultipleTransactions() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsNonNullableColumns() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMinimumSQLGrammar() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsCoreSQLGrammar() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsExtendedSQLGrammar() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsANSI92FullSQL() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
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
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getCatalogSeparator() throws SQLException {
    return ".";
  }

  @Override
  public boolean supportsSchemasInDataManipulation() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
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
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsPositionedUpdate() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsSelectForUpdate() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsStoredProcedures() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInComparisons() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsSubqueriesInExists() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsSubqueriesInIns() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsCorrelatedSubqueries() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
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
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxBinaryLiteralLength() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxCharLiteralLength() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxColumnNameLength() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxColumnsInGroupBy() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxColumnsInIndex() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxColumnsInOrderBy() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxColumnsInSelect() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxColumnsInTable() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxConnections() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxCursorNameLength() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxIndexLength() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxSchemaNameLength() throws SQLException {
    return 32;
  }

  @Override
  public int getMaxProcedureNameLength() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxCatalogNameLength() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxRowSize() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxStatementLength() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxStatements() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxTableNameLength() throws SQLException {
    return TABLE_NAME_LENGTH;
  }

  @Override
  public int getMaxTablesInSelect() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxUserNameLength() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getDefaultTransactionIsolation() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsTransactions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
    return false;
  }

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    return false;
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    return false;
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    return false;
  }

  @Override
  public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
      throws SQLException {
    // Return an empty result set
    OdpsResultSetMetaData meta =
        new OdpsResultSetMetaData(Arrays.asList("PROCEDURE_CAT", "PROCEDURE_SCHEM",
            "PROCEDURE_NAME", "RESERVERD", "RESERVERD", "RESERVERD", "REMARKS", "PROCEDURE_TYPE",
            "SPECIFIC_NAME"), Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.STRING,
            TypeInfoFactory.STRING, TypeInfoFactory.STRING, TypeInfoFactory.STRING,
            TypeInfoFactory.STRING, TypeInfoFactory.STRING, TypeInfoFactory.BIGINT,
            TypeInfoFactory.STRING));

    return new OdpsStaticResultSet(getConnection(), meta);
  }

  @Override
  public ResultSet getProcedureColumns(String catalog, String schemaPattern,
      String procedureNamePattern, String columnNamePattern) throws SQLException {
    // Return an empty result set
    OdpsResultSetMetaData meta =
        new OdpsResultSetMetaData(Arrays.asList("STUPID_PLACEHOLDERS", "USELESS_PLACEHOLDER"),
            Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.STRING));

    return new OdpsStaticResultSet(getConnection(), meta);
  }

  @Override
  public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern,
      String[] types) throws SQLException {

    long begin = System.currentTimeMillis();

    List<Object[]> rows = new ArrayList<Object[]>();

    if (Utils.matchPattern(conn.getOdps().getDefaultProject(), schemaPattern)) {
      try {
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
          Object[] rowVals =
              {null, t.getProject(), tableName, tableType, t.getComment(), null, null, null, null,
                  "USER"};
          rows.add(rowVals);
        }
      } catch (Exception e) {
        log.error("getTables fails: ", e);
        throw new SQLException("getTables fails: ", e);
      }
    }

    long end = System.currentTimeMillis();
    log.debug("It took me " + (end - begin) + " ms to get " + rows.size() + " Tables");

    OdpsResultSetMetaData meta =
        new OdpsResultSetMetaData(Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
            "TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME",
            "SELF_REFERENCING_COL_NAME", "REF_GENERATION"), Arrays.asList(TypeInfoFactory.STRING,
            TypeInfoFactory.STRING, TypeInfoFactory.STRING, TypeInfoFactory.STRING,
            TypeInfoFactory.STRING, TypeInfoFactory.STRING, TypeInfoFactory.STRING,
            TypeInfoFactory.STRING, TypeInfoFactory.STRING, TypeInfoFactory.STRING));

    return new OdpsStaticResultSet(getConnection(), meta, rows.iterator());
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    OdpsResultSetMetaData meta =
        new OdpsResultSetMetaData(Arrays.asList("TABLE_SCHEM", "TABLE_CATALOG"), Arrays.asList(
            TypeInfoFactory.STRING, TypeInfoFactory.STRING));
    List<Object[]> rows = new ArrayList<Object[]>();
    String[] row = {conn.getOdps().getDefaultProject(), DEFAULT_ODPS_CATALOG};
    rows.add(row);
    return new OdpsStaticResultSet(getConnection(), meta, rows.iterator());
  }

  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    OdpsResultSetMetaData meta =
        new OdpsResultSetMetaData(Arrays.asList("TABLE_SCHEM", "TABLE_CATALOG"), Arrays.asList(
            TypeInfoFactory.STRING, TypeInfoFactory.STRING));
    List<Object[]> rows = new ArrayList<Object[]>();
    if (Utils.matchPattern(conn.getOdps().getDefaultProject(), schemaPattern)) {
      String[] row = {conn.getOdps().getDefaultProject(), DEFAULT_ODPS_CATALOG};
      rows.add(row);
    }
    return new OdpsStaticResultSet(getConnection(), meta, rows.iterator());
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    OdpsResultSetMetaData meta =
        new OdpsResultSetMetaData(Arrays.asList("TABLE_CAT"), Arrays.asList(TypeInfoFactory.STRING));
    List<Object[]> rows = new ArrayList<Object[]>();
    String[] row = {conn.getOdps().getEndpoint()};
    rows.add(row);
    return new OdpsStaticResultSet(getConnection(), meta, rows.iterator());

  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    List<Object[]> rows = new ArrayList<Object[]>();
    String[] row = {"TABLE", "VIEW"};
    rows.add(row);

    // Build result set meta data
    OdpsResultSetMetaData meta =
        new OdpsResultSetMetaData(Arrays.asList("TABLE_TYPE"), Arrays.asList(TypeInfoFactory.STRING));

    return new OdpsStaticResultSet(getConnection(), meta, rows.iterator());

  }

  @Override
  public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern,
      String columnNamePattern) throws SQLException {

    long begin = System.currentTimeMillis();

    if (tableNamePattern == null) {
      throw new SQLException("Table name must be given when getColumns");
    }

    List<Object[]> rows = new ArrayList<Object[]>();
    
    if (tableNamePattern != null && !tableNamePattern.trim().isEmpty()
        && !tableNamePattern.trim().equals("%") && !tableNamePattern.trim().equals("*")) {
      try {
        Table table;
        if (StringUtils.isNullOrEmpty(schemaPattern)) {
          table = conn.getOdps().tables().get(tableNamePattern);
        } else {
          table = conn.getOdps().tables().get(schemaPattern, tableNamePattern);
        }
        table.reload();
        // Read column information from table schema
        List<Column> columns = table.getSchema().getColumns();
        for (int i = 0; i < columns.size(); i++) {
          Column col = columns.get(i);
          JdbcColumn jdbcCol =
              new JdbcColumn(col.getName(), tableNamePattern, null, col.getType(), col.getTypeInfo(),
                  col.getComment(), i + 1);
          Object[] rowVals =
              {null, jdbcCol.getTableSchema(), jdbcCol.getTableName(), jdbcCol.getColumnName(),
                  (long) jdbcCol.getType(), jdbcCol.getTypeName(), null, null,
                  (long) jdbcCol.getDecimalDigits(), (long) jdbcCol.getNumPercRaidx(),
                  (long) jdbcCol.getIsNullable(), jdbcCol.getComment(), null, null, null, null,
                  (long) jdbcCol.getOrdinalPos(), jdbcCol.getIsNullableString(), null, null, null,
                  null};

          rows.add(rowVals);
        }
      } catch (OdpsException e) {
        throw new SQLException("catalog=" + catalog + ",schemaPattern=" + schemaPattern
            + ",tableNamePattern=" + tableNamePattern + ",columnNamePattern" + columnNamePattern, e);
      }
    }

    long end = System.currentTimeMillis();
    log.debug("It took me " + (end - begin) + " ms to get " + rows.size() + " columns");

    // Build result set meta data
    OdpsResultSetMetaData meta =
        new OdpsResultSetMetaData(Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
            "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH",
            "DECIMAL_DIGITS", "NUM_PERC_RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF",
            "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION",
            "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE"),
            Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.STRING, TypeInfoFactory.STRING,
                TypeInfoFactory.STRING, TypeInfoFactory.BIGINT, TypeInfoFactory.STRING,
                TypeInfoFactory.BIGINT, TypeInfoFactory.BIGINT, TypeInfoFactory.BIGINT,
                TypeInfoFactory.BIGINT, TypeInfoFactory.BIGINT, TypeInfoFactory.STRING,
                TypeInfoFactory.STRING, TypeInfoFactory.BIGINT, TypeInfoFactory.BIGINT,
                TypeInfoFactory.BIGINT, TypeInfoFactory.BIGINT, TypeInfoFactory.STRING,
                TypeInfoFactory.STRING, TypeInfoFactory.STRING, TypeInfoFactory.STRING,
                TypeInfoFactory.BIGINT));

    return new OdpsStaticResultSet(getConnection(), meta, rows.iterator());
  }

  @Override
  public ResultSet getColumnPrivileges(String catalog, String schema, String table,
      String columnNamePattern) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope,
      boolean nullable) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getVersionColumns(String catalog, String schema, String table)
      throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {

    // Return an empty result set
    OdpsResultSetMetaData meta =
        new OdpsResultSetMetaData(Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
            "COLUMN_NAME", "KEY_SEQ", "PK_NAME"), Arrays.asList(TypeInfoFactory.STRING,
            TypeInfoFactory.STRING, TypeInfoFactory.STRING, TypeInfoFactory.STRING,
            TypeInfoFactory.BIGINT, TypeInfoFactory.STRING));

    return new OdpsStaticResultSet(getConnection(), meta);
  }

  @Override
  public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
    // Return an empty result set
    OdpsResultSetMetaData meta =
        new OdpsResultSetMetaData(Arrays.asList("PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME",
            "PKCOLUMN_NAME", "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME",
            "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY"),
            Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.STRING, TypeInfoFactory.STRING,
                TypeInfoFactory.STRING, TypeInfoFactory.STRING, TypeInfoFactory.STRING,
                TypeInfoFactory.STRING, TypeInfoFactory.STRING, TypeInfoFactory.BIGINT,
                TypeInfoFactory.BIGINT, TypeInfoFactory.BIGINT, TypeInfoFactory.STRING,
                TypeInfoFactory.STRING, TypeInfoFactory.STRING));

    return new OdpsStaticResultSet(getConnection(), meta);
  }

  @Override
  public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
      String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getTypeInfo() throws SQLException {
    // Return an empty result set
    OdpsResultSetMetaData meta =
        new OdpsResultSetMetaData(Arrays.asList("STUPID_PLACEHOLDERS", "USELESS_PLACEHOLDER"),
            Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.STRING));

    return new OdpsStaticResultSet(getConnection(), meta);
  }

  @Override
  public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique,
      boolean approximate) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsResultSetType(int type) throws SQLException {
    if (type == ResultSet.TYPE_FORWARD_ONLY || type == ResultSet.TYPE_SCROLL_INSENSITIVE) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean ownUpdatesAreVisible(int type) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean ownDeletesAreVisible(int type) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean ownInsertsAreVisible(int type) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean othersUpdatesAreVisible(int type) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean othersDeletesAreVisible(int type) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean othersInsertsAreVisible(int type) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean updatesAreDetected(int type) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean deletesAreDetected(int type) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean insertsAreDetected(int type) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsBatchUpdates() throws SQLException {
    return false;
  }

  @Override
  public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
      throws SQLException {

    // Return an empty result set
    OdpsResultSetMetaData meta =
        new OdpsResultSetMetaData(Arrays.asList("TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME",
            "CLASS_NAME", "DATA_TYPE", "REMARKS", "BASE_TYPE"), Arrays.asList(
            TypeInfoFactory.STRING, TypeInfoFactory.STRING, TypeInfoFactory.STRING,
            TypeInfoFactory.STRING, TypeInfoFactory.BIGINT, TypeInfoFactory.STRING,
            TypeInfoFactory.BIGINT));

    return new OdpsStaticResultSet(getConnection(), meta);
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
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsMultipleOpenResults() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsGetGeneratedKeys() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
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
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getDatabaseMajorVersion() throws SQLException {
    try {
      return Integer.parseInt(Utils.retrieveVersion("sdk.version").split("\\.")[0]);
    } catch (Exception e) {
      e.printStackTrace();
      return 1;
    }
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException {
    try {
      return Integer.parseInt(Utils.retrieveVersion("sdk.version").split("\\.")[1]);
    } catch (Exception e) {
      e.printStackTrace();
      return 0;
    }
  }

  @Override
  public int getJDBCMajorVersion() throws SQLException {
    // TODO: risky
    return 4;
  }

  @Override
  public int getJDBCMinorVersion() throws SQLException {
    return 0;
  }

  @Override
  public int getSQLStateType() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean locatorsUpdateCopy() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsStatementPooling() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public RowIdLifetime getRowIdLifetime() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getClientInfoProperties() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
      throws SQLException {

    long begin = System.currentTimeMillis();

    List<Object[]> rows = new ArrayList<Object[]>();
    for (Function f : conn.getOdps().functions()) {
      Object[] rowVals = {null, null, f.getName(), 0, (long) functionResultUnknown, null};
      rows.add(rowVals);
    }

    long end = System.currentTimeMillis();
    log.debug("It took me " + (end - begin) + " ms to get " + rows.size() + " functions");

    OdpsResultSetMetaData meta =
        new OdpsResultSetMetaData(Arrays.asList("FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME",
            "REMARKS", "FUNCTION_TYPE", "SPECIFIC_NAME"), Arrays.asList(TypeInfoFactory.STRING,
            TypeInfoFactory.STRING, TypeInfoFactory.STRING, TypeInfoFactory.STRING,
            TypeInfoFactory.BIGINT, TypeInfoFactory.STRING));

    return new OdpsStaticResultSet(getConnection(), meta, rows.iterator());
  }

  @Override
  public ResultSet getFunctionColumns(String catalog, String schemaPattern,
      String functionNamePattern, String columnNamePattern) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
      String columnNamePattern) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  public boolean generatedKeyAlwaysReturned() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }
}
