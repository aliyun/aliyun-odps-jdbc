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
