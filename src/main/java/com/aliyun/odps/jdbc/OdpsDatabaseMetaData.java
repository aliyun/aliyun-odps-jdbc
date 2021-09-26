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
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;

import com.aliyun.odps.Column;
import com.aliyun.odps.Function;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Table;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.jdbc.utils.JdbcColumn;
import com.aliyun.odps.jdbc.utils.OdpsLogger;
import com.aliyun.odps.jdbc.utils.Utils;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.aliyun.odps.utils.StringUtils;

public class OdpsDatabaseMetaData extends WrapperAdapter implements DatabaseMetaData {

  private final OdpsLogger log;
  private static final String PRODUCT_NAME = "MaxCompute/ODPS";
  private static final String DRIVER_NAME = "odps-jdbc";

  private static final String SCHEMA_TERM = "project";
  private static final String CATALOG_TERM = "project";
  private static final String PROCEDURE_TERM = "N/A";

  // Table types
  public static final String TABLE_TYPE_TABLE = "TABLE";
  public static final String TABLE_TYPE_VIEW = "VIEW";

  // Column names
  public static final String COL_NAME_TABLE_CAT = "TABLE_CAT";
  public static final String COL_NAME_TABLE_CATALOG = "TABLE_CATALOG";
  public static final String COL_NAME_TABLE_SCHEM = "TABLE_SCHEM";
  public static final String COL_NAME_TABLE_NAME = "TABLE_NAME";
  public static final String COL_NAME_TABLE_TYPE = "TABLE_TYPE";
  public static final String COL_NAME_REMARKS = "REMARKS";
  public static final String COL_NAME_TYPE_CAT = "TYPE_CAT";
  public static final String COL_NAME_TYPE_SCHEM = "TYPE_SCHEM";
  public static final String COL_NAME_TYPE_NAME = "TYPE_NAME";
  public static final String COL_NAME_SELF_REFERENCING_COL_NAME = "SELF_REFERENCING_COL_NAME";
  public static final String COL_NAME_REF_GENERATION = "REF_GENERATION";

  // MaxCompute public data set project
  public static final String PRJ_NAME_MAXCOMPUTE_PUBLIC_DATA = "MAXCOMPUTE_PUBLIC_DATA";

  private static final int TABLE_NAME_LENGTH = 128;

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
    return true;
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
    return false;
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsPositionedDelete() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsPositionedUpdate() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSelectForUpdate() throws SQLException {
    return false;
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
    return 32;
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
                                                "PROCEDURE_NAME", "RESERVERD", "RESERVERD",
                                                "RESERVERD", "REMARKS", "PROCEDURE_TYPE",
                                                "SPECIFIC_NAME"),
                                  Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.STRING,
                                                TypeInfoFactory.STRING, TypeInfoFactory.STRING,
                                                TypeInfoFactory.STRING,
                                                TypeInfoFactory.STRING, TypeInfoFactory.STRING,
                                                TypeInfoFactory.BIGINT,
                                                TypeInfoFactory.STRING));

    return new OdpsStaticResultSet(getConnection(), meta);
  }

  @Override
  public ResultSet getProcedureColumns(String catalog, String schemaPattern,
                                       String procedureNamePattern, String columnNamePattern)
      throws SQLException {
    // Return an empty result set
    OdpsResultSetMetaData meta =
        new OdpsResultSetMetaData(Arrays.asList("STUPID_PLACEHOLDERS", "USELESS_PLACEHOLDER"),
                                  Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.STRING));

    return new OdpsStaticResultSet(getConnection(), meta);
  }

  @Override
  public ResultSet getTables(
      String catalog,
      String schemaPattern,
      String tableNamePattern,
      String[] types) throws SQLException {
    long begin = System.currentTimeMillis();
    List<Object[]> rows = new ArrayList<>();

    try {
      if (!conn.getTables().isEmpty()) {
        for (Entry<String, List<String>> entry : conn.getTables().entrySet()) {
          LinkedList<String> tables = new LinkedList<>();
          String projectName = entry.getKey();
          if (!catalogMatches(catalog, projectName)
              || !schemaMatches(schemaPattern, projectName)) {
            continue;
          }
          for (String tableName : entry.getValue()) {
            if (Utils.matchPattern(tableName, tableNamePattern)
                && conn.getOdps().tables().exists(projectName, tableName)) {
              tables.add(tableName);
            }
          }
          if (tables.size() > 0) {
            convertTableNamesToRows(types, rows, projectName, tables);
          }
        }
      } else {
        ResultSet schemas = getSchemas(catalog, schemaPattern);
        List<Table> tables = new LinkedList<>();

        // Iterate through all the available catalog & schemas
        while (schemas.next()) {
          if (catalogMatches(catalog, schemas.getString(COL_NAME_TABLE_CATALOG))
              && schemaMatches(schemaPattern, schemas.getString(COL_NAME_TABLE_SCHEM))) {
            // Enable the argument 'extended' so that the returned table objects contains all the
            // information needed by JDBC, like comment and type.
            Iterator<Table> iter = conn.getOdps().tables().iterator(
                schemas.getString(COL_NAME_TABLE_SCHEM), null, true);
            while (iter.hasNext()) {
              Table t = iter.next();
              String tableName = t.getName();
              if (!Utils.matchPattern(tableName, tableNamePattern)) {
                continue;
              }
              tables.add(t);
              if (tables.size() == 100) {
                convertTablesToRows(types, rows, tables);
              }
            }
          }
        }
        if (tables.size() > 0) {
          convertTablesToRows(types, rows, tables);
        }
        schemas.close();
      }
    } catch (Exception e) {
      throw new SQLException(e);
    }

    long end = System.currentTimeMillis();
    log.info("It took me " + (end - begin) + " ms to get " + rows.size() + " Tables");

    OdpsResultSetMetaData meta =
        new OdpsResultSetMetaData(
            Arrays.asList(
                COL_NAME_TABLE_CAT,
                COL_NAME_TABLE_SCHEM,
                COL_NAME_TABLE_NAME,
                COL_NAME_TABLE_TYPE,
                COL_NAME_REMARKS,
                COL_NAME_TYPE_CAT,
                COL_NAME_TYPE_SCHEM,
                COL_NAME_TYPE_NAME,
                COL_NAME_SELF_REFERENCING_COL_NAME,
                COL_NAME_REF_GENERATION),
            Arrays.asList(
                TypeInfoFactory.STRING,
                TypeInfoFactory.STRING,
                TypeInfoFactory.STRING,
                TypeInfoFactory.STRING,
                TypeInfoFactory.STRING,
                TypeInfoFactory.STRING,
                TypeInfoFactory.STRING,
                TypeInfoFactory.STRING,
                TypeInfoFactory.STRING,
                TypeInfoFactory.STRING));

    sortRows(rows, new int[]{3, 0, 1, 2});
    return new OdpsStaticResultSet(getConnection(), meta, rows.iterator());
  }

  private boolean catalogMatches(String catalog, String actual) {
    return catalog == null || catalog.equalsIgnoreCase(actual);
  }

  private boolean schemaMatches(String schemaPattern, String actual) {
    return Utils.matchPattern(actual, schemaPattern);
  }

  private void convertTableNamesToRows(
      String[] types,
      List<Object[]> rows,
      String projectName,
      List<String> names)
      throws OdpsException {
    LinkedList<Table> tables = new LinkedList<>();
    tables.addAll(conn.getOdps().tables().loadTables(projectName, names));
    convertTablesToRows(types, rows, tables);
  }

  private void convertTablesToRows(String[] types, List<Object[]> rows, List<Table> tables) {
    for (Table t : tables) {
      String tableType = t.isVirtualView() ? TABLE_TYPE_VIEW : TABLE_TYPE_TABLE;
      if (types != null && types.length != 0) {
        if (!Arrays.asList(types).contains(tableType)) {
          continue;
        }
      }
      Object[] rowVals = {
          t.getProject(),
          t.getProject(),
          t.getName(),
          tableType,
          t.getComment(),
          null, null, null, null, null};
      rows.add(rowVals);
    }
    tables.clear();
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    return getSchemas(null, null);
  }

  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    OdpsResultSetMetaData meta = new OdpsResultSetMetaData(
        Arrays.asList(COL_NAME_TABLE_SCHEM, COL_NAME_TABLE_CATALOG),
        Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.STRING));
    List<Object[]> rows = new ArrayList<>();

    // In MaxCompute, catalog == schema == project.
    String schema = catalog;

    // Project MAXCOMPUTE_PUBLIC_DATA includes MaxCompute public data sets. It is available in
    // almost all the regions of every public MaxCompute service, but may not be available in
    // private services.
    try {
      if (catalogMatches(catalog, PRJ_NAME_MAXCOMPUTE_PUBLIC_DATA)
          && schemaMatches(schemaPattern, PRJ_NAME_MAXCOMPUTE_PUBLIC_DATA)
          && conn.getOdps().projects().exists(PRJ_NAME_MAXCOMPUTE_PUBLIC_DATA)) {
        rows.add(new String[]{PRJ_NAME_MAXCOMPUTE_PUBLIC_DATA, PRJ_NAME_MAXCOMPUTE_PUBLIC_DATA});
      }
    } catch (OdpsException e) {
      String errMsg = "Failed to access project: " + e.getMessage();
      conn.log.debug(errMsg);
    }

    try {
      if (catalog == null) {
        // The follow code block implements the actual interface DatabaseMetaData#getCatalogs. But
        // since list projects is quite slow right now, this impl is commented out.
//        for (Project p : conn.getOdps().projects().iterable(null)) {
//          if (!PRJ_NAME_MAXCOMPUTE_PUBLIC_DATA.equals(p.getName())
//              && Utils.matchPattern(p.getName(), schemaPattern)) {
//            rows.add(new String[]{p.getName(), p.getName()});
//          }
//        }
        if (!PRJ_NAME_MAXCOMPUTE_PUBLIC_DATA.equalsIgnoreCase(conn.getOdps().getDefaultProject())) {
          rows.add(
              new String[]{conn.getOdps().getDefaultProject(), conn.getOdps().getDefaultProject()});
        }
      } else {
        if (!PRJ_NAME_MAXCOMPUTE_PUBLIC_DATA.equalsIgnoreCase(schema)
            && Utils.matchPattern(schema, schemaPattern)
            && conn.getOdps().projects().exists(schema)) {
          rows.add(new String[]{schema, schema});
        }
      }
    } catch (OdpsException | RuntimeException e) {
      throw new SQLException(e);
    }

    sortRows(rows, new int[]{1, 0});
    return new OdpsStaticResultSet(getConnection(), meta, rows.iterator());
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    OdpsResultSetMetaData meta = new OdpsResultSetMetaData(
        Collections.singletonList(COL_NAME_TABLE_CAT),
        Collections.singletonList(TypeInfoFactory.STRING));
    List<Object[]> rows = new ArrayList<>();

    // Project MAXCOMPUTE_PUBLIC_DATA includes MaxCompute public data sets. It is available in
    // almost all the regions of every public MaxCompute service, but may not be available in
    // private services.
    try {
      if (conn.getOdps().projects().exists(PRJ_NAME_MAXCOMPUTE_PUBLIC_DATA)) {
        rows.add(new String[]{PRJ_NAME_MAXCOMPUTE_PUBLIC_DATA});
      }
    } catch (OdpsException e) {
      String errMsg = "Failed to access project: " + e.getMessage();
      conn.log.debug(errMsg);
    }

    // The follow code block implements the actual interface DatabaseMetaData#getCatalogs. But since
    // list projects is quite slow right now, this impl is commented out.
//    try {
//      for (Project p : conn.getOdps().projects().iterable(null)) {
//        rows.add(new String[] {p.getName()});
//      }
//    } catch (RuntimeException e) {
//      throw new SQLException(e);
//    }

    if (!PRJ_NAME_MAXCOMPUTE_PUBLIC_DATA.equalsIgnoreCase(conn.getOdps().getDefaultProject())) {
      rows.add(new String[]{conn.getOdps().getDefaultProject()});
    }

    sortRows(rows, new int[]{0});
    return new OdpsStaticResultSet(getConnection(), meta, rows.iterator());
  }

  /**
   * Sort rows by specified columns.
   *
   * @param rows          Rows. Elements in the list cannot be null and must have the same length.
   * @param columnsToSort Indexes of columns to sort.
   */
  private void sortRows(List<Object[]> rows, int[] columnsToSort) {
    rows.sort((row1, row2) -> {
      Objects.requireNonNull(row1);
      Objects.requireNonNull(row2);
      if (row1.length != row2.length) {
        throw new IllegalArgumentException("Rows have different length");
      }

      for (int i = 0; i < row1.length; i++) {
        for (int idx : columnsToSort) {
          if (row1[idx] != null && row2[idx] != null) {
            int ret = ((String) row1[idx]).compareTo((String) row2[idx]);
            if (ret == 0) {
              continue;
            }
            return ret;
          } else if (row1[idx] != null && row2[idx] == null) {
            return 1;
          } else if (row1[idx] == null && row2[idx] != null) {
            return -1;
          }
        }
      }

      return 0;
    });
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    List<Object[]> rows = new ArrayList<>();

    OdpsResultSetMetaData meta = new OdpsResultSetMetaData(
        Arrays.asList(COL_NAME_TABLE_TYPE),
        Arrays.asList(TypeInfoFactory.STRING));

    rows.add(new String[]{TABLE_TYPE_TABLE});
    rows.add(new String[]{TABLE_TYPE_VIEW});

    return new OdpsStaticResultSet(getConnection(), meta, rows.iterator());
  }

  @Override
  public ResultSet getColumns(
      String catalog,
      String schemaPattern,
      String tableNamePattern,
      String columnNamePattern) throws SQLException {

    long begin = System.currentTimeMillis();

    if (tableNamePattern == null) {
      throw new SQLException("Table name must be given when getColumns");
    }

    List<Object[]> rows = new ArrayList<Object[]>();

    if (!tableNamePattern.trim().isEmpty() && !"%".equals(tableNamePattern.trim())
        && !"*".equals(tableNamePattern.trim())) {
      try {
        Table table;
        if (StringUtils.isNullOrEmpty(schemaPattern)) {
          table = conn.getOdps().tables().get(tableNamePattern);
        } else {
          table = conn.getOdps().tables().get(schemaPattern, tableNamePattern);
        }
        table.reload();

        // Read column & partition column information from table schema
        List<Column> columns = new LinkedList<>();
        columns.addAll(table.getSchema().getColumns());
        columns.addAll(table.getSchema().getPartitionColumns());
        for (int i = 0; i < columns.size(); i++) {
          Column col = columns.get(i);
          JdbcColumn jdbcCol = new JdbcColumn(col.getName(),
                                              tableNamePattern,
                                              table.getProject(),
                                              col.getTypeInfo().getOdpsType(),
                                              col.getTypeInfo(),
                                              col.getComment(),
                                              i + 1);
          Object[] rowVals =
              {catalog, jdbcCol.getTableSchema(), jdbcCol.getTableName(), jdbcCol.getColumnName(),
               (long) jdbcCol.getType(), jdbcCol.getTypeName(), null, null,
               (long) jdbcCol.getDecimalDigits(), (long) jdbcCol.getNumPercRaidx(),
               (long) jdbcCol.getIsNullable(), jdbcCol.getComment(), null, null, null, null,
               (long) jdbcCol.getOrdinalPos(), jdbcCol.getIsNullableString(), null, null, null,
               null};

          rows.add(rowVals);
        }
      } catch (OdpsException e) {
        throw new SQLException("catalog=" + catalog + ",schemaPattern=" + schemaPattern
                               + ",tableNamePattern=" + tableNamePattern + ",columnNamePattern"
                               + columnNamePattern, e);
      }
    }

    long end = System.currentTimeMillis();
    log.info("It took me " + (end - begin) + " ms to get " + rows.size() + " columns");

    // Build result set meta data
    OdpsResultSetMetaData meta =
        new OdpsResultSetMetaData(Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
                                                "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
                                                "COLUMN_SIZE", "BUFFER_LENGTH",
                                                "DECIMAL_DIGITS", "NUM_PERC_RADIX", "NULLABLE",
                                                "REMARKS", "COLUMN_DEF",
                                                "SQL_DATA_TYPE", "SQL_DATETIME_SUB",
                                                "CHAR_OCTET_LENGTH", "ORDINAL_POSITION",
                                                "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA",
                                                "SCOPE_TABLE", "SOURCE_DATA_TYPE"),
                                  Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.STRING,
                                                TypeInfoFactory.STRING,
                                                TypeInfoFactory.STRING, TypeInfoFactory.BIGINT,
                                                TypeInfoFactory.STRING,
                                                TypeInfoFactory.BIGINT, TypeInfoFactory.BIGINT,
                                                TypeInfoFactory.BIGINT,
                                                TypeInfoFactory.BIGINT, TypeInfoFactory.BIGINT,
                                                TypeInfoFactory.STRING,
                                                TypeInfoFactory.STRING, TypeInfoFactory.BIGINT,
                                                TypeInfoFactory.BIGINT,
                                                TypeInfoFactory.BIGINT, TypeInfoFactory.BIGINT,
                                                TypeInfoFactory.STRING,
                                                TypeInfoFactory.STRING, TypeInfoFactory.STRING,
                                                TypeInfoFactory.STRING,
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
                                                "COLUMN_NAME", "KEY_SEQ", "PK_NAME"),
                                  Arrays.asList(TypeInfoFactory.STRING,
                                                TypeInfoFactory.STRING, TypeInfoFactory.STRING,
                                                TypeInfoFactory.STRING,
                                                TypeInfoFactory.BIGINT, TypeInfoFactory.STRING));

    return new OdpsStaticResultSet(getConnection(), meta);
  }

  @Override
  public ResultSet getImportedKeys(String catalog, String schema, String table)
      throws SQLException {
    // Return an empty result set
    OdpsResultSetMetaData meta =
        new OdpsResultSetMetaData(Arrays.asList("PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME",
                                                "PKCOLUMN_NAME", "FKTABLE_CAT", "FKTABLE_SCHEM",
                                                "FKTABLE_NAME", "FKCOLUMN_NAME",
                                                "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE", "FK_NAME",
                                                "PK_NAME", "DEFERRABILITY"),
                                  Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.STRING,
                                                TypeInfoFactory.STRING,
                                                TypeInfoFactory.STRING, TypeInfoFactory.STRING,
                                                TypeInfoFactory.STRING,
                                                TypeInfoFactory.STRING, TypeInfoFactory.STRING,
                                                TypeInfoFactory.BIGINT,
                                                TypeInfoFactory.BIGINT, TypeInfoFactory.BIGINT,
                                                TypeInfoFactory.STRING,
                                                TypeInfoFactory.STRING, TypeInfoFactory.STRING));

    return new OdpsStaticResultSet(getConnection(), meta);
  }

  @Override
  public ResultSet getExportedKeys(String catalog, String schema, String table)
      throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
                                     String foreignCatalog, String foreignSchema,
                                     String foreignTable) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getTypeInfo() throws SQLException {
    List<String> columnNames =
        Arrays.asList("TYPE_NAME", "DATA_TYPE", "PRECISION",
                      "LITERAL_PREFIX", "LITERAL_SUFFIX", "CREATE_PARAMS",
                      "NULLABLE", "CASE_SENSITIVE", "SEARCHABLE",
                      "UNSIGNED_ATTRIBUTE", "FIXED_PREC_SCALE", "AUTO_INCREMENT",
                      "LOCAL_TYPE_NAME", "MINIMUM_SCALE", "MAXIMUM_SCALE",
                      "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "NUM_PREC_RADIX");
    List<TypeInfo> columnTypes =
        Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.INT, TypeInfoFactory.INT,
                      TypeInfoFactory.STRING, TypeInfoFactory.STRING, TypeInfoFactory.STRING,
                      TypeInfoFactory.SMALLINT, TypeInfoFactory.BOOLEAN, TypeInfoFactory.SMALLINT,
                      TypeInfoFactory.BOOLEAN, TypeInfoFactory.BOOLEAN, TypeInfoFactory.BOOLEAN,
                      TypeInfoFactory.STRING, TypeInfoFactory.SMALLINT, TypeInfoFactory.SMALLINT,
                      TypeInfoFactory.INT, TypeInfoFactory.INT, TypeInfoFactory.INT);
    OdpsResultSetMetaData meta = new OdpsResultSetMetaData(columnNames, columnTypes);

    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{TypeInfoFactory.TINYINT.getTypeName(), Types.TINYINT, 3,
                          null, "Y", null,
                          typeNullable, null, typePredBasic,
                          false, false, false,
                          null, 0, 0,
                          null, null, 10});
    rows.add(new Object[]{TypeInfoFactory.SMALLINT.getTypeName(), Types.SMALLINT, 5,
                          null, "S", null,
                          typeNullable, null, typePredBasic,
                          false, false, false,
                          null, 0, 0,
                          null, null, 10});
    rows.add(new Object[]{TypeInfoFactory.INT.getTypeName(), Types.INTEGER, 10,
                          null, null, null,
                          typeNullable, null, typePredBasic,
                          false, false, false,
                          null, 0, 0,
                          null, null, 10});
    rows.add(new Object[]{TypeInfoFactory.BIGINT.getTypeName(), Types.BIGINT, 19,
                          null, "L", null,
                          typeNullable, null, typePredBasic,
                          false, false, false,
                          null, 0, 0,
                          null, null, 10});
    rows.add(new Object[]{TypeInfoFactory.BINARY.getTypeName(), Types.BINARY, 8 * 1024 * 1024,
                          null, null, null,
                          typeNullable, null, typePredNone,
                          false, false, false,
                          null, 0, 0,
                          null, null, null});
    rows.add(new Object[]{TypeInfoFactory.FLOAT.getTypeName(), Types.FLOAT, null,
                          null, null, null,
                          typeNullable, null, typePredBasic,
                          false, false, false,
                          null, null, null,
                          null, null, 2});
    rows.add(new Object[]{TypeInfoFactory.DOUBLE.getTypeName(), Types.DOUBLE, null,
                          null, null, null,
                          typeNullable, null, typePredBasic,
                          false, false, false,
                          null, null, null,
                          null, null, 2});
    rows.add(new Object[]{TypeInfoFactory.DECIMAL.getTypeName(), Types.DECIMAL, 38,
                          null, "BD", null,
                          typeNullable, null, typePredBasic,
                          false, true, false,
                          null, 18, 18,
                          null, null, 10});
    rows.add(new Object[]{"VARCHAR", Types.VARCHAR, null,
                          null, null, "PRECISION",
                          typeNullable, true, typePredChar,
                          false, false, false,
                          null, null, null,
                          null, null, null});
    rows.add(new Object[]{"CHAR", Types.CHAR, null,
                          null, null, "PRECISION",
                          typeNullable, true, typePredChar,
                          false, false, false,
                          null, null, null,
                          null, null, null});
    rows.add(new Object[]{TypeInfoFactory.STRING, Types.VARCHAR, 8 * 1024 * 1024,
                          "\"", "\"", null,
                          typeNullable, true, typePredChar,
                          false, false, false,
                          null, null, null,
                          null, null, null});
    // yyyy-mm-dd
    rows.add(new Object[]{TypeInfoFactory.DATE, Types.DATE, 10,
                          "DATE'", "'", null,
                          typeNullable, null, typePredBasic,
                          false, false, false,
                          null, null, null,
                          null, null, null});
    // yyyy-mm-dd hh:MM:ss.SSS
    rows.add(new Object[]{TypeInfoFactory.DATETIME, Types.TIMESTAMP, 23,
                          "DATETIME'", "'", null,
                          typeNullable, null, typePredBasic,
                          false, false, false,
                          null, null, null,
                          null, null, null});
    // yyyy-mm-dd hh:MM:ss.SSSSSSSSS
    rows.add(new Object[]{TypeInfoFactory.TIMESTAMP, Types.TIMESTAMP, 29,
                          "TIMESTAMP'", "'", null,
                          typeNullable, null, typePredBasic,
                          false, false, false,
                          null, null, null,
                          null, null, null});
    rows.add(new Object[]{TypeInfoFactory.BOOLEAN, Types.BOOLEAN, null,
                          null, null, null,
                          typeNullable, null, typePredBasic,
                          false, false, false,
                          null, null, null,
                          null, null, null});
    return new OdpsStaticResultSet(getConnection(), meta, rows.iterator());
  }

  @Override
  public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique,
                                boolean approximate) throws SQLException {
    OdpsResultSetMetaData meta =
        new OdpsResultSetMetaData(
            Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
                          "NON_UNIQUE", "INDEX_QUALIFIER", "INDEX_NAME",
                          "TYPE", "ORDINAL_POSITION", "COLUMN_NAME",
                          "ASC_OR_DESC", "CARDINALITY", "PAGES",
                          "FILTER_CONDITION"),
            Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.STRING, TypeInfoFactory.STRING,
                          TypeInfoFactory.BOOLEAN, TypeInfoFactory.STRING, TypeInfoFactory.STRING,
                          TypeInfoFactory.SMALLINT, TypeInfoFactory.SMALLINT,
                          TypeInfoFactory.STRING,
                          TypeInfoFactory.STRING, TypeInfoFactory.BIGINT, TypeInfoFactory.BIGINT,
                          TypeInfoFactory.STRING));

    // Return an empty result set since index is unsupported in MaxCompute
    return new OdpsStaticResultSet(getConnection(), meta, Collections.emptyIterator());
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
    return false;
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
  public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern,
                           int[] types)
      throws SQLException {
    // Return an empty result set
    OdpsResultSetMetaData meta =
        new OdpsResultSetMetaData(Arrays.asList("TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME",
                                                "CLASS_NAME", "DATA_TYPE", "REMARKS", "BASE_TYPE"),
                                  Arrays.asList(
                                      TypeInfoFactory.STRING, TypeInfoFactory.STRING,
                                      TypeInfoFactory.STRING,
                                      TypeInfoFactory.STRING, TypeInfoFactory.BIGINT,
                                      TypeInfoFactory.STRING,
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
    log.info("It took me " + (end - begin) + " ms to get " + rows.size() + " functions");

    OdpsResultSetMetaData meta =
        new OdpsResultSetMetaData(Arrays.asList("FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME",
                                                "REMARKS", "FUNCTION_TYPE", "SPECIFIC_NAME"),
                                  Arrays.asList(TypeInfoFactory.STRING,
                                                TypeInfoFactory.STRING, TypeInfoFactory.STRING,
                                                TypeInfoFactory.STRING,
                                                TypeInfoFactory.BIGINT, TypeInfoFactory.STRING));

    return new OdpsStaticResultSet(getConnection(), meta, rows.iterator());
  }

  @Override
  public ResultSet getFunctionColumns(String catalog, String schemaPattern,
                                      String functionNamePattern, String columnNamePattern)
      throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
                                    String columnNamePattern) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean generatedKeyAlwaysReturned() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }
}