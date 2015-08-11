# JDBC Driver for ODPS

## Quickstart

1\. Explictly load the ODPS JDBC driver using Class.`forName()`.
    
For example:
    
    Class.forName("com.aliyun.odps.jdbc.OdpsDriver");


2\. Connect to the ODPS by creating a `Connection` object with the JDBC driver:

For example:
    
    Connection conn = DriverManager.getConnection("jdbc:odps:<ODPS_URL>", config);

The ODPS server works with HTTP or HTTPS protocol, so an `ODPS_URL` looks like:

* `http://<domain>/<subdomain>`
* `https://<domain>/<subdomain>`

If the user does not specify the protocal, HTTPS will be used.

For example:

    Connection conn = DriverManager.getConnection("jdbc:odps://<domain>/<subdomain>", config);


And other information is passed through a `config`. 

For example:

    Properties config = new Properties();
    config.put("access_id", "...");
    config.put("access_key", "...");
    config.put("project_name", "...");
       

3\. Submit SQL to ODPS by creating `Statement` object and using its `executeQuery()` method.

For example:

    Statement stmt = cnct.createStatement();
    ResultSet rset = stmt.executeQuery("SELECT foo FROM bar");

4\. Process the result set.

For example
    
    while (rs.next()) {
        ...
    }
 

## Data Type

| ODPS Type   | Java Type   | JDBC Interface               | JDBC            |  
| :-------: | :-------- | :-------------------- | :-----------: |
| BIGINT      | Long         | int, short, long              | BIGINT        |
| DOUBLE      | Double       | double, float                 | DOUBLE         |
| BOOLEAN     | Boolean     | boolean                        | BOOLEAN       |
| DATETIME    | util.Date    | sql.Date/Time/Timestamp    | TIMESTAMP     |
| STRING      | byte[]       | String                        | VARCHAR       |
| DECIMAL     | math.BigDecimal  | math.BigDecimal       | DECIMAL        |

The data value can be accessed by the getters of `ResultSet` like `getInt()`, `getTime()`, etc.

The implicit type conversion follows the rule:


| ODPS        | BIGINT | DOUBLE | BOOLEAN | DATETIME | STRING | DECIMAL |
| :-------: | :----: | :-----: | :-----: |:-------: |:-----: |:------: |
| boolean     | Y | Y | Y |   | Y |   |
| byte         | Y | Y |   |   |   | Y |
| int          | Y | Y |   |   | Y | Y |
| short        | Y | Y |   |   | Y | Y |
| long         | Y | Y |   |   | Y | Y |
| double       | Y | Y |   |   | Y | Y |
| float        | Y | Y |   |   | Y | Y |
| BigDecimal  |   |   |   |   | Y | Y |
| String       | Y | Y | Y | Y | Y | Y |
| Date         |   |   |   | Y | Y |   |
| Time         |   |   |   | Y | Y |   |
| Timestamp   |   |   |   | Y | Y |   |


## JDBC 4.0 Comliance RoadMap


### DataSource 

TODO

### Driver

Fully implented with the exception of the following methods:

* getMajorVersion
* getMinorVersion

And the following methods whose absence is permitted:

* getParentLogger

### Connection

Fully implented with the exception of the following methods:

* setHoldability
* getHoldability
* isValid
* nativeSQL
* setTransactionIsolation
* commit
* rollback
* prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
* prepareCall
* setReadOnly
* setAutoCommit 

### Statement

Fully implented with the exception of the following methods:

* addBatch
* clearBatch
* executeBatch
* closeOnCompletion
* setEscapeProcessing
* setMaxFieldSize
* setPoolable
* getMoreResults()
* getQueryTimeout
* setQueryTimeout
* ~~setFetchDirection(Statement.FETCH_REVERSE)~~


The parts ODPS JDBC supports but Hive does not:

* getUpdateCount


### ResultSet


* ResultSet Concurrency 
  * CONCUR_READ_ONLY 
  * ~~CONCUR_UPDATABLE~~
* ResultSet Holdablity 
  * ~~HOLD_CURSORS_OVER_COMMIT~~
  * ~~CLOSE_CURSORS_AT_COMMIT~~
* ResultSet Type
  * TYPE_FORWARD_ONLY 
  * TYPE_SCROLL_INSENSITIVE
    * FETCH_FORWARD
    * FETCH_REVERSE (Hive partially supported)
    * FETCH_UNKNOWN
  * ~~TYPE_SCROLL_SENSITIVE~~

Fully implented with the exception of the following methods:

* absolute
* afterLast
* ~~beforeFirst~~
* first
* isAfterLast
* ~~isBeforeFirst~~
* isFirst
* isLast
* last
* previous
* relative

