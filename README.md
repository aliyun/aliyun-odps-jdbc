# JDBC Driver for ODPS




## Installation

	git clone ....
	cd odps-jdbc
	mvn install -Dmaven.test.skip=true


## Getting Started

1\. Explictly load the ODPS JDBC driver using `Class.forName()`.
    
For example:
    
    Class.forName("com.aliyun.odps.jdbc.OdpsDriver");


2\. Connect to the ODPS by creating a `Connection` object with the JDBC driver:

    
    Connection conn = DriverManager.getConnection(url, accessId, accessKey);

The ODPS server works with HTTP (HTTPS recommended) service, so the url looks like:

    String url = "jdbc:odps:https://your.endpoint.domain/subdomain?project=PROJECT_NAME&charset=UTF-8"


`project`, `accessId`, `accessKey` and other information (e.g. `logview`) can also be passed through `Properties`. 

For example:
    
    Properties config = new Properties();
    config.put("access_id", "...");
    config.put("access_key", "...");
    config.put("project_name", "...");
    config.put("charset", "...");
    Connection conn = DriverManager.getConnection("jdbc:odps:<endpoint>", config);
       

3\. Submit SQL to ODPS by creating `Statement` object and using its `executeQuery()` method.

For example:

    Statement stmt = cnct.createStatement();
    ResultSet rset = stmt.executeQuery("SELECT foo FROM bar");

4\. Process the result set.

For example
    
    while (rs.next()) {
        ...
    }
 


## Example 


### JDBC Client Sample Code

    import java.sql.SQLException;
    import java.sql.Connection;
    import java.sql.ResultSet;
    import java.sql.Statement;
    import java.sql.DriverManager;
     
    public class OdpsJdbcClient {
      private static String driverName = "com.aliyun.odps.jdbc.OdpsDriver";
     
      /**
       * @param args
       * @throws SQLException
       */
      public static void main(String[] args) throws SQLException {
        try {
          Class.forName(driverName);
        } catch (ClassNotFoundException e) {
          e.printStackTrace();
          System.exit(1);
        }
    
        // fill in the information here
        String accessId = "your_access_id";
        String accessKey = "your_access_key";
        Connection con = DriverManager.getConnection("jdbc:odps:https://service-corp.odps.aliyun-inc.com/api?project=<your_project_name>", accessId, accessKey);
        Statement stmt = con.createStatement();
        String tableName = "testOdpsDriverTable";
        stmt.execute("drop table if exists " + tableName);
        stmt.execute("create table " + tableName + " (key int, value string)");
    
        String sql;
        ResultSet res;
    
        // insert a record
        sql = String.format("insert into table %s select 24 key, 'hours' value from (select count(1) from %s) a", tableName, tableName);
        System.out.println("Running: " + sql);
        int count = stmt.executeUpdate(sql);
        System.out.println("updated records: " + count);
        
        // select * query
        sql = "select * from " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
          System.out.println(String.valueOf(res.getInt(1)) + "\t" + res.getString(2));
        }
     
        // regular query
        sql = "select count(1) from " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
          System.out.println(res.getString(1));
        }
      }
    }

### Running the JDBC Sample Code

    # compile the client code
    javac OdpsJdbcClient.java
    
    # run the program with specifying the class path
    java -cp odps-jdbc-*-with-dependencies.jar:. OdpsJdbcClient


## Authors && Contributors

- [Cheng Yichao](https://github.com/onesuper)
- [Li Ruibo](https://github.com/lyman)
- [Wen Shaojin](https://github.com/wenshao)

## License

licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html)


## Appendix: Supported APIs

### Connection

|                                                                          | 功能                                                                                     | 实现                                                                                                                      | 测试 |
|--------------------------------------------------------------------------|------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------|------|
| getMetaData()                                                            | 获取一个 DatabaseMetaData 对象，该对象包含关于此 Connection 对象所连接的数据库的元数据。 |                                                                                                                           | Y    |
| createStatement()                                                        | 创建一个 Statement 对象来将 SQL 语句发送到数据库。                                       |                                                                                                                           | Y    |
| createStatement(int resultSetType, int resultSetConcurrency)             | 创建一个 Statement 对象，该对象将生成具有给定类型和并发性的 ResultSet 对象。             | resultSetType允许值：TYPE_SCROLL_INSENSITIVE，ResultSet.TYPE_FORWARD_ONLY ； resultSetConcurrency允许值：CONCUR_READ_ONLY | Y    |
| prepareStatement(String sql)                                             | 创建一个 PreparedStatement 对象来将参数化的 SQL 语句发送到数据库。                       |                                                                                                                           | Y    |
| prepareStatement(String sql, int resultSetType,int resultSetConcurrency) | 创建一个 PreparedStatement 对象，该对象将生成具有给定类型和并发性的 ResultSet 对象。     | resultSetType允许值：TYPE_SCROLL_INSENSITIVE，ResultSet.TYPE_FORWARD_ONLY；resultSetConcurrency允许值：CONCUR_READ_ONLY   | Y    |
| setCatalog(String catalog)                                               | 设置给定目录名称，以便选择要在其中进行工作的此 Connection 对象数据库的子空间。           | 对应 ODPS Endpoint                                                                                                        | Y    |
| getCatalog()                                                             | 获取此 Connection 对象的当前目录名称。                                                   |                                                                                                                           | Y    |
| setSchema(String schema)                                                 | JDK 1.7+                                                                                 | 对应 ODPS Project                                                                                                         | Y    |
| getSchema()                                                              | JDK 1.7+                                                                                 |                                                                                                                           | Y    |
| close()                                                                  | 立即释放此 Connection 对象的数据库和 JDBC 资源，而不是等待它们被自动释放。               |                                                                                                                           |      |
| isClosed()                                                               | 查询此 Connection 对象是否已经被关闭。                                                   |                                                                                                                           |      |


### Statement

|                                  	| 功能                                                                                                              	| 实现             	| 测试 	|
|----------------------------------	|-------------------------------------------------------------------------------------------------------------------	|------------------	|------	|
| executeQuery(String sql)         	| 执行给定的 SQL 语句，该语句返回单个 ResultSet 对象。                                                              	|                  	| Y    	|
| executeUpdate(String sql)        	| 执行给定 SQL 语句，该语句可能为 INSERT、UPDATE 或 DELETE 语句，或者不返回任何内容的 SQL 语句（如 SQL DDL 语句）。 	|                  	| Y    	|
| execute(String sql)              	| 执行给定的 SQL 语句，该语句可能返回多个结果。                                                                     	| 至多返回一个结果 	| Y    	|
| getResultSet()                   	| 以 ResultSet 对象的形式获取当前结果。                                                                             	|                  	| Y    	|
| getUpdateCount()                 	| 以更新计数的形式获取当前结果；如果结果为 ResultSet 对象或没有更多结果，则返回 -1。                                	|                  	| Y    	|
| setFetchDirection(int direction) 	| 向驱动程序提供关于方向的提示，在使用此 Statement 对象创建的 ResultSet 对象中将按该方向处理行。                    	|                  	| Y    	|
| getFetchDirection()              	| 获取从数据库表获取行的方向，该方向是根据此 Statement 对象生成的结果集合的默认值。                                 	|                  	| Y    	|
| setFetchSize()                   	| 为 JDBC 驱动程序提供一个提示，它提示此 Statement 生成的 ResultSet 对象需要更多行时应该从数据库获取的行数。        	|                  	| Y    	|
| getFetchSize()                   	| 获取结果集合的行数，该数是根据此 Statement 对象生成的 ResultSet 对象的默认获取大小。                              	|                  	| Y    	|
| setMaxRows(int max)              	| 将此 Statement 对象生成的所有 ResultSet 对象可以包含的最大行数限制设置为给定数。                                  	|                  	| Y    	|
| getMaxRows()                     	| 获取由此 Statement 对象生成的 ResultSet 对象可以包含的最大行数。                                                  	|                  	| Y    	|
| getConnection()                  	| 获取生成此 Statement 对象的 Connection 对象。                                                                     	|                  	| Y    	|
| close()                          	| 立即释放此 Statement 对象的数据库和 JDBC 资源，而不是等待该对象自动关闭时发生此操作。                             	|                  	|      	|
| isClosed()                       	| 获取是否已关闭了此 Statement 对象。                                                                               	|                  	|      	|
| cancel()                         	| 如果 DBMS 和驱动程序都支持中止 SQL 语句，则取消此 Statement 对象。                                                	|                  	| Y    	|

### ResultSet

|                                  | 功能                                                                                        | 实现                             | 测试 |
|----------------------------------|---------------------------------------------------------------------------------------------|----------------------------------|------|
| getMetaData()                    | 获取此 ResultSet 对象的列的编号、类型和属性。                                               |                                  | Y    |
| getRow()                         | 获取当前行编号。                                                                            |                                  | Y    |
| getType()                        | 获取此 ResultSet 对象的类型。                                                               |                                  |  Y  |
| findColumn(String columnLabel)   | 将给定的 ResultSet 列标签映射到其 ResultSet 列索引。                                        |                                  |  Y   |
| getObject(int columnIndex)       | 以 Java 编程语言中 Object 的形式获取此 ResultSet 对象的当前行中指定列的值。                 |                                  | Y    |
| getObject(int columnLabel)       |                                                                                             |                                  | Y    |
| wasNull()                        | 报告最后一个读取的列是否具有值 SQL NULL。                                                   |                                  |      |
| getBigDecimal(int columnIndex)   | 以具有全精度的 java.math.BigDecimal 的形式获取此 ResultSet 对象的当前行中指定列的值。       |                                  | Y    |
| getBigDecimal(int columnLabel)   |                                                                                             |                                  | Y    |
| getBoolean(int columnIndex)      | 以 Java 编程语言中 boolean 的形式获取此 ResultSet 对象的当前行中指定列的值。                |                                  | Y    |
| getBoolean(String columnLabel)   |                                                                                             |                                  | Y    |
| getByte(int columnIndex)         | 以 Java 编程语言中 byte 的形式获取此 ResultSet 对象的当前行中指定列的值。                   |                                  | Y    |
| getByte(String columnLabel)      |                                                                                             |                                  | Y    |
| getDate(int columnIndex)         | 以 Java 编程语言中 java.sql.Date 对象的形式获取此 ResultSet 对象的当前行中指定列的值。      |                                  | Y    |
| getDate(String columnLabel)      |                                                                                             |                                  | Y    |
| getDouble(int columnIndex)       | 以 Java 编程语言中 double 的形式获取此 ResultSet 对象的当前行中指定列的值。                 |                                  | Y    |
| getDouble(String columnLabel）   |                                                                                             |                                  | Y    |
| getFloat(int columnIndex)        | 以 Java 编程语言中 float 的形式获取此 ResultSet 对象的当前行中指定列的值。                  |                                  | Y    |
| getFloat(int columnLabel)        |                                                                                             |                                  | Y    |
| getInt(int columnIndex)          | 以 Java 编程语言中 int 的形式获取此 ResultSet 对象的当前行中指定列的值。                    |                                  | Y    |
| getInt(int columnLabel)          |                                                                                             |                                  | Y    |
| getLong(int columnIndex)         | 以 Java 编程语言中 long 的形式获取此 ResultSet 对象的当前行中指定列的值。                   |                                  | Y    |
| getLong(int columnLabel)         |                                                                                             |                                  | Y    |
| getShort(int columnIndex)        | 以 Java 编程语言中 short 的形式获取此 ResultSet 对象的当前行中指定列的值。                  |                                  | Y    |
| getShort(int columnLabel)        |                                                                                             |                                  | Y    |
| getString(int columnIndex)       | 以 Java 编程语言中 String 的形式获取此 ResultSet 对象的当前行中指定列的值。                 |                                  | Y    |
| getString(int columnLabel)       |                                                                                             |                                  | Y    |
| getTime(int columnIndex)         | 以 Java 编程语言中 java.sql.Time 对象的形式获取此 ResultSet 对象的当前行中指定列的值。      |                                  | Y    |
| getTime(int columnLabel)         |                                                                                             |                                  | Y    |
| getTimestamp(int columnIndex)    | 以 Java 编程语言中 java.sql.Timestamp 对象的形式获取此 ResultSet 对象的当前行中指定列的值。 |                                  | Y    |
| getTimestamp(int columnLabel)    |                                                                                             |                                  | Y    |
| next()                           | 将光标从当前位置向前移一行。                                                                |                                  |   Y  |
| absolute(int row)                |  将光标移动到此 ResultSet 对象的给定行编号。                                                                                            |  只对TYPE_SCROLL_INSENSITIVE有效 | Y    |
| relative(int rows)               | 按相对行数（或正或负）移动光标。                                                            | 只对TYPE_SCROLL_INSENSITIVE有效  | Y    |
| afterLast()                     | 将光标移动到此 ResultSet 对象的末尾，正好位于最后一行之后。 | 只对TYPE_SCROLL_INSENSITIVE有效  | Y    |
| first()                          | 将光标移动到此 ResultSet 对象的第一行。                                                                                           | 只对TYPE_SCROLL_INSENSITIVE有效  | Y    |
| isAfterLast()                    | 获取光标是否位于此 ResultSet 对象的最后一行之后。                                           | 只对TYPE_SCROLL_INSENSITIVE有效  | Y    |
| isBeforeFirst()                  | 获取光标是否位于此 ResultSet 对象的第一行之前。                                             | 只对TYPE_SCROLL_INSENSITIVE有效  | Y    |
| isFirst()                        | 获取光标是否位于此 ResultSet 对象的第一行。                                                 | 只对TYPE_SCROLL_INSENSITIVE有效  | Y    |
| isLast()                         | 获取光标是否位于此 ResultSet 对象的最后一行。                                               | 只对TYPE_SCROLL_INSENSITIVE有效  | Y    |
| last()                           | 将光标移动到此 ResultSet 对象的最后一行。                                                   | 只对TYPE_SCROLL_INSENSITIVE有效  | Y    |
| previous()                       | 将光标移动到此 ResultSet 对象的上一行。                                                     | 只对TYPE_SCROLL_INSENSITIVE有效  | Y    |
| setFetchSize(int rows)           | 为 JDBC 驱动程序设置此 ResultSet 对象需要更多行时应该从数据库获取的行数。                   | 只对TYPE_SCROLL_INSENSITIVE有效  | Y    |
| getFetchSize()                   | 获取此 ResultSet 对象的获取大小。                                                           | 只对TYPE_SCROLL_INSENSITIVE有效  | Y    |
| setFetchDirection(int direction) | 设置此 ResultSet 对象中行的处理方向。                                                       | 只对TYPE_SCROLL_INSENSITIVE有效  | Y    |
| getFetchDirection()              | 获取此 ResultSet 对象的获取方向。                                                           | 只对TYPE_SCROLL_INSENSITIVE有效  | Y    |
| close()                          | 立即释放此 ResultSet 对象的数据库和 JDBC 资源，而不是等待该对象自动关闭时发生此操作。       |                                  |      |
| isClosed()                       | 获取此 ResultSet 对象是否已关闭。                                                           |                                  |      |


### DatabaseMetaData

|                                                                                                     | 功能                                       | 实现                              | 测试 |
|-----------------------------------------------------------------------------------------------------|--------------------------------------------|-----------------------------------|------|
| getConnection()                                                                                     | 获取此元数据对象所产生的连接。             |                                   |      |
| getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)            | 获取可在给定类别中使用的表的描述。         | 支持参数：tableNamePattern，types | Y    |
| getFunctions(String catalog, String schemaPattern, String functionNamePattern)                      | 获取给定类别中可用的系统和用户函数的描述。 | 有效参数：                        |      |
| getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) | 获取可在指定类别中使用的表列的描述。       | 支持参数：                        |      |
| getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)                  | 获取在特定模式中定义的用户定义类型 (UDT) 的描述。       | 返回空表                        |      |
| getImportedKeys(String catalog, String schema, String table)                                         | 获取由给定表的外键列（表导入的主键）引用的主键列的描述。 | 返回空表                        |      |
| getPrimaryKeys(String catalog, String schema, String table)                                         | 获取对给定表的主键列的描述。       | 返回空表                        |      |
| getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) | 获取给定类别的存储过程参数和结果列的描述。 | 返回空表                        |      |
| getTypeInfo()                      | 获取此数据库支持的所有数据类型的描述。 | 返回空表                        |      |
| getTableTypes()                    | 获取可在此数据库中使用的表类型。       | 返回空表                        |      |
| getSchemaTerm()                    | 获取数据库供应商用于 "schema" 的首选术语。| "project"                        |      |
| getProcedureTerm()                 | 获取数据库供应商用于 "procedure" 的首选术语。 | "UDT"                        |      |
| getCatalogTerm()                   | 获取数据库供应商用于 "catalog" 的首选术语。 | "endpoint"                        |      |
| getCatalogs()                       | 获取可在此数据库中使用的类别名称。 | 返回空表                        |      |
| getSchemas()                       | 获取可在此数据库中使用的模式名称。 | 返回空表                        |      |
| getSchemas(String catalog, String schemaPattern)    | 获取此数据库中可用的模式名称。 | 返回空表                        |      |


### ResultSetMetaData

|                                  | 功能                                                                                      | 实现           | 测试 |
|----------------------------------|-------------------------------------------------------------------------------------------|----------------|------|
| getColumnClassName(int column)   | 如果调用方法 ResultSet.getObject 从列中获取值，则返回构造其实例的 Java 类的完全限定名称。 |                | Y    |
| getColumnName(int column)        | 获取指定列的名称。                                                                        |                | Y    |
| getColumnCount()                 | 返回此 ResultSet 对象中的列数。                                                           |                | Y    |
| getColumnDisplaySize(int column) | 指示指定列的最大标准宽度，以字符为单位。                                                  |                | Y    |
| getColumnTypeName(int column)    | 获取指定列的数据库特定的类型名称。                                                        |                | Y    |
| getColumnType(int column)        | 获取指定列的 SQL 类型。                                                                   |                | Y    |
| getPrecision(int column)         | 获取指定列的指定列宽。                                                                    |                |      |
| getScale(int column)             | 获取指定列的小数点右边的位数。                                                            |                |      |
| getCatalogName(int column)       | 获取指定列的表目录名称。                                                                  | 返回空字符串   |      |
| getSchemaName(int column)        | 获取指定列的表模式。                                                                      | 返回空字符串   |      |
| getTableName(int column)         | 获取指定列的名称。                                                                        | 返回空字符串   |      |
| isAutoIncrement(int column)      | 指示是否自动为指定列进行编号。                                                            | fasle          |      |
| isCaseSensitive(int column)      | 指示列的大小写是否有关系。                                                                | 取决于数据类型 |      |
| isCurrency(int column)           | 指示指定的列是否是一个哈希代码值。                                                        | false          |      |
| isDefinitelyWritable(int column) | 指示在指定的列上进行写操作是否明确可以获得成功。                                          | false          |      |
| isNullable(int column)           | 指示指定列中的值是否可以为 null。                                                         | true           |      |
| isReadOnly(int column)           | 指示指定的列是否明确不可写入。                                                            | true           |      |
| isSearchable(int column)         | 指示是否可以在 where 子句中使用指定的列。                                                 | true           |      |
| isSigned(int column)             | 指示指定列中的值是否带正负号。                                                            |                |      |
| isWritable(int column)           | 指示在指定的列上进行写操作是否可以获得成功。                                              | false          |      |