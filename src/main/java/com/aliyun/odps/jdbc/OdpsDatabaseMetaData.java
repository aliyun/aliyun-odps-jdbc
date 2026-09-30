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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import com.aliyun.odps.Column;
import com.aliyun.odps.Function;
import com.aliyun.odps.Odps;
import com.aliyun.odps.NoSuchObjectException;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableFilter;
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

  private static final String SCHEMA_TERM = "schema";
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
    // getTables(), getColumns() and getSchemas() all match their pattern arguments through
    // Utils.matchPattern, which treats a backslash as the escape for the SQL wildcards. A caller
    // that has to ask for a name holding a literal '%' or '_' can only write that escape once it
    // can look it up, so this reports the character the driver actually honours instead of
    // claiming the whole question is unsupported.
    return "\\";
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
    return conn.isOdpsNamespaceSchema();
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    return conn.isOdpsNamespaceSchema();
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    return conn.isOdpsNamespaceSchema();
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    return conn.isOdpsNamespaceSchema();
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    return conn.isOdpsNamespaceSchema();
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
    log.info("getTables called: catalog=" + catalog
             + ", schemaPattern=" + schemaPattern
             + ", tableNamePattern=" + tableNamePattern
             + ", types=" + (types == null ? "null" : Arrays.toString(types)));

    // Performance optimization: when caller (e.g. Tableau) does not specify a schema
    // and the connection has a default schema configured via the URL `schema` parameter,
    // restrict the lookup to that schema. This avoids iterating tables across every
    // schema in the project. Only applies in odps namespace schema mode.
    if (schemaPattern == null
        && Boolean.TRUE.equals(conn.isOdpsNamespaceSchema())
        && conn.getSchema() != null) {
      log.info("getTables: schemaPattern is null, restricting to connection schema: "
               + conn.getSchema());
      schemaPattern = conn.getSchema();
    }

    List<Object[]> rows = new ArrayList<>();

    try {
      if (!conn.getTables().isEmpty()) {
        for (Entry<String, Map<String, List<String>>> entry: conn.getTables().entrySet()) {
          String projectName = entry.getKey();
          if (!catalogMatches(catalog, projectName)) {
            continue;
          }
          for (Entry<String, List<String>> tableEntry: entry.getValue().entrySet()) {
            LinkedList<String> tables = new LinkedList<>();
            String schemaName = tableEntry.getKey();
            if (!schemaCriterionMatches(schemaPattern, schemaName)) {
              continue;
            }
            for (String tableName : tableEntry.getValue()) {
              if (Utils.matchPattern(tableName, tableNamePattern)) {
                tables.add(tableName);
              }
            }
            if (tables.size() > 0) {
              convertTableNamesToRows(types, rows, projectName, schemaName, tables);
            }
          }
        }
      } else {
        ResultSet schemas = getSchemas(catalog, schemaPattern);
        List<Table> tables = new LinkedList<>();

        // Push tableNamePattern down to the server as a prefix filter when possible.
        // TableFilter.setName matches by prefix; the unescaped literal prefix preceding the
        // first SQL wildcard is a safe over-approximation, refined by client-side matchPattern.
        TableFilter tableFilter = null;
        String namePrefix = extractLiteralPrefix(tableNamePattern);
        if (namePrefix != null && !namePrefix.isEmpty()) {
          tableFilter = new TableFilter();
          tableFilter.setName(namePrefix);
        }

        // Iterate through all the available catalog & schemas
        while (schemas.next()) {
          if (catalogMatches(catalog, schemas.getString(COL_NAME_TABLE_CATALOG))
              && schemaCriterionMatches(schemaPattern, schemas.getString(COL_NAME_TABLE_SCHEM))) {
            // Enable the argument 'extended' so that the returned table objects contains all the
            // information needed by JDBC, like comment and type.
            String schemaCatalog = schemas.getString(COL_NAME_TABLE_CATALOG);
            String schemaName = schemas.getString(COL_NAME_TABLE_SCHEM);
            Iterator<Table> iter = conn.getOdps().tables().iterator(
                schemaCatalog, schemaName, tableFilter, true);
            try {
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
            } catch (Exception e) {
              // Some schemas may contain tables with invalid XML characters in comments,
              // causing the SDK iterator to fail. Skip the problematic schema and continue.
              log.warn("getTables: failed to list tables in schema "
                       + schemaCatalog + "." + schemaName + ": " + e.getMessage());
            }
          }
        }
        if (tables.size() > 0) {
          convertTablesToRows(types, rows, tables);
        }
        schemas.close();
      }
    } catch (Exception e) {
      log.error("getTables failed: catalog=" + catalog
                + ", schemaPattern=" + schemaPattern
                + ", tableNamePattern=" + tableNamePattern, e);
      throw new SQLException(e.getMessage(), e);
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

  /**
   * Name a caller may use for the single implicit schema of a two-tier project, when it learned
   * that name from a driver that answered {@code getSchemas()} with it.
   */
  private static final String TWO_TIER_SCHEMA_ALIAS = "default";

  /** Service error code for "project or schema not found" (ODPS-0420111). */
  private static final String ERROR_CODE_PROJECT_OR_SCHEMA_MISSING = "ODPS-0420111";

  /** Service error code for "table not found" (ODPS-0130131). */
  private static final String ERROR_CODE_TABLE_MISSING = "ODPS-0130131";

  /**
   * Whether a caller's schema criterion admits {@code actualSchema}.
   *
   * <p>Plain pattern matching, plus one compatibility allowance: in a two-tier project the
   * implicit schema is published under the project name (that is the value {@link #getTables} and
   * {@link #getColumns} put in {@code TABLE_SCHEM}), and a caller that was told "default" by an
   * older driver still matches. Namespace-schema projects expose real schemas and get no such
   * allowance -- {@code default} is a schema that exists there, not an alias.
   */
  private boolean schemaCriterionMatches(String schemaPattern, String actualSchema) {
    if (Utils.matchPattern(actualSchema, schemaPattern)) {
      return true;
    }
    return !conn.isOdpsNamespaceSchema()
        && conn.getOdps().getDefaultProject().equalsIgnoreCase(actualSchema)
        && Utils.matchPattern(TWO_TIER_SCHEMA_ALIAS, schemaPattern);
  }

  /**
   * Returns true if {@code pattern} contains any unescaped SQL wildcard ({@code %} or
   * {@code _}). Backslash-escaped wildcards are treated as literals, matching the semantics
   * of {@link Utils#matchPattern(String, String)}.
   */
  private static boolean hasWildcard(String pattern) {
    if (pattern == null) {
      return false;
    }
    for (int i = 0; i < pattern.length(); i++) {
      char c = pattern.charAt(i);
      if ((c == '%' || c == '_') && (i == 0 || pattern.charAt(i - 1) != '\\')) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns the literal prefix of {@code pattern} preceding the first unescaped SQL wildcard,
   * with {@code \%} and {@code \_} unescaped. Returns null when the pattern is null/empty.
   */
  private static String extractLiteralPrefix(String pattern) {
    if (StringUtils.isNullOrEmpty(pattern)) {
      return null;
    }
    StringBuilder sb = new StringBuilder(pattern.length());
    for (int i = 0; i < pattern.length(); i++) {
      char c = pattern.charAt(i);
      if (c == '\\' && i + 1 < pattern.length()) {
        char next = pattern.charAt(i + 1);
        if (next == '%' || next == '_') {
          sb.append(next);
          i++;
          continue;
        }
      }
      if (c == '%' || c == '_') {
        break;
      }
      sb.append(c);
    }
    return sb.toString();
  }

  /**
   * Removes backslash escapes for {@code %} and {@code _} from a wildcard-free pattern.
   */
  private static String unescapePattern(String pattern) {
    if (pattern == null || pattern.indexOf('\\') < 0) {
      return pattern;
    }
    return pattern.replace("\\%", "%").replace("\\_", "_");
  }

  private void convertTableNamesToRows(
      String[] types,
      List<Object[]> rows,
      String projectName,
      String schemaName,
      List<String> names)
      throws OdpsException {
    LinkedList<Table> tables = new LinkedList<>();
    tables.addAll(conn.getOdps().tables().loadTables(projectName, schemaName, names));
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
      String schemaName = t.getProject();
      if (conn.isOdpsNamespaceSchema()) {
        schemaName = t.getSchemaName();
      }
      Object[] rowVals = {
          t.getProject(),
          schemaName,
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
    log.info("getSchemas called: catalog=" + catalog + ", schemaPattern=" + schemaPattern);
    /** ResultSet Format:
     *  odpsnamespace=true
     *  TABLE_SCHEM    TABLE_CATALOG
     *  -----------    -----------
     *  schemaName     projectName
     *
     * odpsnamespace=false
     *  TABLE_SCHEM    TABLE_CATALOG
     *  -----------    -----------
     *  projectName    projectName
     */
    OdpsResultSetMetaData meta = new OdpsResultSetMetaData(
        Arrays.asList(COL_NAME_TABLE_SCHEM, COL_NAME_TABLE_CATALOG),
        Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.STRING));
    List<Object[]> rows = new ArrayList<>();

    // In MaxCompute, catalog == project.
    String schema = catalog;

    try {
      if (!conn.isOdpsNamespaceSchema()) {
        // A two-tier project exposes one implicit schema under the one catalog getCatalogs()
        // reports. Three things used to be wrong here at once: any string at all came back as a
        // schema row (even a project that is not this connection's), a schemaPattern that matches
        // nothing still returned that row, and the name given for the schema disagreed with the
        // TABLE_SCHEM that getTables() and getColumns() publish for the very same tables.
        String projectName = conn.getOdps().getDefaultProject();
        if (catalogMatches(catalog, projectName)
            && schemaCriterionMatches(schemaPattern, projectName)) {
          rows.add(new String[]{projectName, catalog != null ? catalog : projectName});
        }
      } else {
        if (catalog == null) {
          catalog = conn.getOdps().getDefaultProject();
        }
        // Performance optimization: when caller passes an exact schema name (no wildcard),
        // skip the `show schemas` SQL job — it can take tens of seconds in projects with
        // thousands of schemas. We trust the caller; downstream getTables on a non-existent
        // schema simply returns empty.
        if (schemaPattern != null && !hasWildcard(schemaPattern)) {
          rows.add(new String[]{unescapePattern(schemaPattern), catalog});
        } else {
          List<String> schemaList =
              Utils.getSchemaList(conn.getOdps(), "show schemas in " + catalog + ";");
          for (String schemaName: schemaList) {
            if (schemaMatches(schemaPattern, schemaName)) {
              rows.add(new String[]{schemaName, catalog});
            }
          }
        }
      }
    } catch (Exception e) {
      if (isMissingObject(e)) {
        // Asking for the schemas of a project that is not there is an unmatched search
        // criterion, the same way getTables() already answers it -- empty, not an error.
        log.info("getSchemas: catalog does not exist, returning an empty result set: " + catalog);
      } else {
        throw new SQLException(e.getMessage(), e);
      }
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

    rows.add(new String[]{conn.getOdps().getDefaultProject()});
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

  /**
   * Locate the single table {@link #getColumns} describes.
   *
   * <p>In the three-tier model a null {@code schemaPattern} means "the schema this connection is
   * sitting in", the same reading {@link #getTables} uses when it restricts an unrestricted call
   * to {@code conn.getSchema()}. Without that, {@code getColumns(c, null, t)} and {@code
   * getTables(c, null, t)} can describe two different tables that share the name {@code t}, and
   * the caller has no way to notice.
   */
  private Table resolveTableForColumns(String catalog, String schemaPattern,
                                       String tableNamePattern) throws SQLException {
    Odps odps = conn.getOdps();
    if (conn.isOdpsNamespaceSchema()) {
      String project = catalog != null ? catalog : odps.getDefaultProject();
      String schema = schemaPattern != null ? schemaPattern : conn.getSchema();
      return odps.tables().get(project, schema, tableNamePattern);
    }
    if (StringUtils.isNullOrEmpty(catalog)) {
      return odps.tables().get(tableNamePattern);
    }
    return odps.tables().get(catalog, tableNamePattern);
  }

  /**
   * True when the service answered "this object does not exist" (project, schema or table), as
   * opposed to a failure the caller has to hear about. The SDK reports a missing table as {@link
   * NoSuchObjectException}, while a missing project or schema reaches us as the underlying
   * {@link com.aliyun.odps.rest.RestException} carrying the {@code NoSuchObject} error code.
   */
  private static boolean isMissingObject(Throwable t) {
    for (Throwable cur = t; cur != null; cur = cur.getCause()) {
      if (cur instanceof NoSuchObjectException) {
        return true;
      }
      if (cur instanceof OdpsException
          && "NoSuchObject".equalsIgnoreCase(((OdpsException) cur).getErrorCode())) {
        return true;
      }
      // A statement the service rejects because the object named in it is absent -- "show
      // schemas in <project>", for instance -- reaches us as a plain message, not as a typed
      // exception, so the service error code inside the text is all there is to judge on.
      String text = cur.getMessage();
      if (text != null && (text.contains(ERROR_CODE_PROJECT_OR_SCHEMA_MISSING)
          || text.contains(ERROR_CODE_TABLE_MISSING)
          || text.contains("Code=NoSuchObject"))) {
        return true;
      }
      if (cur instanceof com.aliyun.odps.rest.RestException) {
        com.aliyun.odps.rest.ErrorMessage em =
            ((com.aliyun.odps.rest.RestException) cur).getErrorMessage();
        if (em != null && "NoSuchObject".equalsIgnoreCase(em.getErrorcode())) {
          return true;
        }
      }
      if (cur.getCause() == cur) {
        break;
      }
    }
    return false;
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
        Table table = resolveTableForColumns(catalog, schemaPattern, tableNamePattern);
        table.reload();

        // Read column & partition column information from table schema
        List<Column> columns = new LinkedList<>();
        columns.addAll(table.getSchema().getColumns());
        columns.addAll(table.getSchema().getPartitionColumns());
        for (int i = 0; i < columns.size(); i++) {
          Column col = columns.get(i);
          String colSchema = table.getProject();
          if (conn.isOdpsNamespaceSchema()) {
            colSchema = table.getSchemaName();
          }
          if (!Utils.matchPattern(col.getName(), columnNamePattern)) {
            // JDBC: only the columns whose name matches columnNamePattern are returned.
            continue;
          }
          JdbcColumn jdbcCol = new JdbcColumn(col.getName(),
                                              table.getName(),
                                              colSchema,
                                              col.getTypeInfo().getOdpsType(),
                                              col.getTypeInfo(),
                                              col.getComment(),
                                              i + 1);
          Object[] rowVals =
              {table.getProject(), // table catalog (odps project), as getTables() reports it
               jdbcCol.getTableSchema(), // table schema
               jdbcCol.getTableName(), // table name
               jdbcCol.getColumnName(), // column name
               (long) jdbcCol.getType(), // SQL type from java.sql.Types
               jdbcCol.getTypeName(), // Data source dependent type name, actually odps typeInfo name
               jdbcCol.getColumnSize(), // column size, null for the types MaxCompute does not bound
               null, // not used
               (long) jdbcCol.getDecimalDigits(), // the number of fractional digits.
               (long) jdbcCol.getNumPercRaidx(), // Radix (typically either 10 or 2)
               (long) jdbcCol.getIsNullable(), // is NULL allowed.
               jdbcCol.getComment(), // comment describing column (may be null)
               null, // default value for the column
               null, // unused
               null, // unused
               null, // for char types the maximum number of bytes in the column
               (long) jdbcCol.getOrdinalPos(), // index of column in table (start at 1)
               jdbcCol.getIsNullableString(), // ISO rules are used to determine the nullability for a column.
               null, // SCOPE_CATALOG
               null, // SCOPE_SCHEMA
               null, // SCOPE_TABLE
               null, // SOURCE_DATA_TYPE
               "NO", // IS_AUTOINCREMENT
               "NO", // IS_GENERATEDCOLUMN
               };

          rows.add(rowVals);
        }
      } catch (OdpsException e) {
        if (isMissingObject(e)) {
          // The project / schema / table simply is not there. Every other JDBC metadata method
          // of this driver (getTables, getSchemas, getPrimaryKeys, ...) answers an unmatched
          // search criterion with an empty result set, and java.sql.DatabaseMetaData documents
          // these calls as "only those ... matching the given criteria are returned" -- not as
          // an error. A caller probing an object it is not sure exists gets the same answer as
          // the table list gives, and a real failure (permission, transport, service error)
          // still surfaces below.
          log.info("getColumns: object does not exist, returning an empty result set: catalog="
                   + catalog + ", schemaPattern=" + schemaPattern + ", tableNamePattern="
                   + tableNamePattern);
        } else {
          throw new SQLException("catalog=" + catalog + ",schemaPattern=" + schemaPattern
                                 + ",tableNamePattern=" + tableNamePattern + ",columnNamePattern"
                                 + columnNamePattern, e);
        }
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
                                                "SCOPE_TABLE", "SOURCE_DATA_TYPE",
                                                "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN"),
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
                                                TypeInfoFactory.BIGINT, TypeInfoFactory.STRING,
                                                TypeInfoFactory.STRING));

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