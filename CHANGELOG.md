### v1.7 (2016-03-18)
1. Log for SQLFeatureNotSupportedException on the critical path.
1. Refactor the unitest

### v1.6 (2015-12-31)

1. Put logview in the warning chain to let be captured in **SQLWorkbench/j**.
1. Remove the hardcode when retrieve version
1. Clear javadoc warnings.
1. Put all the utils to `/utils` folder.

### v1.5 (2015-12-30)

1. Set temp table lifecycle in create SQL (eliminate the round trip cost).
1. Accept `VALUES(?, ?, ?)` in prepareStatement.
1. Reuse one tunnel upload session in `PrepareStatement.executeBatch()`. Commit the session on the close of the PrepareStatement.
1. Catch the NPE of `instance.getTaskStatus().get("SQL")`.
1. Support `supportsSchemasInTableDefinitions` to adpat to **Kettle**.

### v1.4 (2015-12-22)

1. Support `isValid()` to adpat to **HikariCP**.
1. Support `setReadOnly()` to adapt to **HikariCP** (only allow for setting it to false).
1. Add an example to use **HikariCP**.

### v1.3 (2015-12-07)

1. Support `supportsResultSetType()` to adapt to **Pentaho**.
1. Support `PreparedStatement.setBytes()`.
1. Support `getMaxTableNameLength()` to adapt to **SQL Workbench/J**.
1. Support `getIdentifierQuoteString()` to adapt to **SQL Workbench/J**.
1. Fix the `getUpdateCount()` bug. See Issue \#15.

### v1.2 (2015-11-29)

1. Support batch insert in PreparedStatement.
1. Add an example to migrate data using batch functionality. The example splits the source and uses more than 1 thread to upload.
1. `DatabaseMetaData.getColumns()` Support accoss schema query.
1. Per-connection logger bug fixed.
1. Display uuid in loggers' console handler.
1. Use travis to deploy.
1. `user_agent` looks like odps-jdbc-1.1.
1. Rename OdpsQueryResultSet to ScollResultSet.
1. Use project name in log string.
1. Throw NPE if `instance.getStatus() == null`.


### v1.1 (2015-11-17)

1. SQLWorkbench's wbcopy command supported.
1. Compress data when downloading the result set.
1. No longer use log4j.
1. `Driver.getParentLogger()` supported. 
1. Per-connection logger (support two connections with different log level.) 
1. More debug logs for profiling.
1. Change the driver name to 'ODPS'.
1. Optimize the performance of getting tables.
1. No longer supports the concept of catalog (all related functions return null).
