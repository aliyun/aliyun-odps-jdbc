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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.aliyun.odps.Function;
import com.aliyun.odps.Functions;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.Table;
import com.aliyun.odps.Tables;
import com.aliyun.odps.account.AliyunAccount;

public class OdpsDatabaseMetaData extends WrapperAdapter implements DatabaseMetaData {

  private OdpsConnection conn;

  OdpsDatabaseMetaData(OdpsConnection conn) {
    this.conn = conn;
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
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getDriverName() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getDriverVersion() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getDriverMajorVersion() {
    return 0;
  }

  @Override
  public int getDriverMinorVersion() {
    return 0;
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
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getNumericFunctions() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getStringFunctions() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getSystemFunctions() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getTimeDateFunctions() throws SQLException {
    throw new SQLFeatureNotSupportedException();
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
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getProcedureTerm() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getCatalogTerm() throws SQLException {
    throw new SQLFeatureNotSupportedException();
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
    throw new SQLFeatureNotSupportedException();
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
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    throw new SQLFeatureNotSupportedException();
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
    throw new SQLFeatureNotSupportedException();
  }

  // TODO
  @Override
  public ResultSet getProcedureColumns(String catalog, String schemaPattern,
                                       String procedureNamePattern, String columnNamePattern)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  // TODO
  @Override
  public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern,
                             String[] types) throws SQLException {

    Tables tables = conn.getOdps().tables();

    Iterator<Table> iterator;
    if (catalog == null || catalog.isEmpty()) {
      iterator = tables.iterator();
    } else {
      iterator = tables.iterator(catalog);
    }

    List<String> columnNames = Arrays.asList(
        "TABLE_CAT",
        "TABLE_SCHEM",
        "TABLE_NAME",
        "TABLE_TYPE",
        "REMARKS",
        "TYPE_CAT",
        "TYPE_SCHEM",
        "TYPE_NAME",
        "SELF_REFERENCING_COL_NAME",
        "REF_GENERATION"
    );

    List<OdpsType> columnSqlTypes = Arrays.asList(
        OdpsType.STRING,
        OdpsType.STRING,
        OdpsType.STRING,
        OdpsType.STRING,
        OdpsType.STRING,
        OdpsType.STRING,
        OdpsType.STRING,
        OdpsType.STRING,
        OdpsType.STRING,
        OdpsType.STRING
    );

    OdpsResultSetMetaData meta = new OdpsResultSetMetaData(columnNames, columnSqlTypes);
    return new OdpsTablesResultSet(iterator, meta);
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    return null;
  }

  // TODO
  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  // TODO
  @Override
  public ResultSet getCatalogs() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  // TODO
  @Override
  public ResultSet getTableTypes() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  // TODO
  @Override
  public ResultSet getColumns(String catalog, String schemaPattern,
                              String tableNamePattern, String columnNamePattern)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
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

  // TODO
  @Override
  public ResultSet getPrimaryKeys(String catalog, String schema, String table)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  // TODO
  @Override
  public ResultSet getImportedKeys(String catalog, String schema, String table)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
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

  // TODO
  @Override
  public ResultSet getTypeInfo() throws SQLException {
    throw new SQLFeatureNotSupportedException();
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
    throw new SQLFeatureNotSupportedException();
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

  // TODO
  @Override
  public ResultSet getFunctions(String catalog, String schemaPattern,
                                String functionNamePattern) throws SQLException {

    Functions functions = conn.getOdps().functions();
    Iterator<Function> iterator;

    if (catalog == null || catalog.isEmpty()) {
      iterator = functions.iterator();
    } else {
      iterator = functions.iterator(catalog);
    }

    List<String> columnNames = Arrays.asList(
        "FUNCTION_CAT",
        "FUNCTION_SCHEM",
        "FUNCTION_NAME",
        "REMARKS",
        "FUNCTION_TYPE",
        "SPECIFIC_NAME"
    );

    List<OdpsType> columnSqlTypes = Arrays.asList(
        OdpsType.STRING,
        OdpsType.STRING,
        OdpsType.STRING,
        OdpsType.STRING,
        OdpsType.BIGINT,  // Short indeed
        OdpsType.STRING
    );

    OdpsResultSetMetaData meta = new OdpsResultSetMetaData(columnNames, columnSqlTypes);
    return new OdpsFunctionsResultSet(iterator, meta);
  }

  @Override
  public ResultSet getFunctionColumns(String catalog, String schemaPattern,
                                      String functionNamePattern, String columnNamePattern)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
                                    String columnNamePattern) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean generatedKeyAlwaysReturned() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }
}
