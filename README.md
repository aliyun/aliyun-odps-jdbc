# JDBC Driver for ODPS



## Installation

### Requirements

* Java 6+



### Build


	git clone ....
	cd odps-jdbc
	mvn install -Dmaven.test.skip=true


## Quickstart

1\. Explictly load the ODPS JDBC driver using `Class.forName()`.
    
For example:
    
    Class.forName("com.aliyun.odps.jdbc.OdpsDriver");


2\. Connect to the ODPS by creating a `Connection` object with the JDBC driver:

    
    Connection conn = DriverManager.getConnection(url, accessId, accessKey);

The ODPS server works with HTTP or HTTPS protocol, so the url looks like: `jdbc:odps:<endpoint>@<project>`

For example:

* `jdbc:odps:http://www.aliyun.com/service@foobar`
* `jdbc:odps:https://www.aliyun.com/service@foobar`

`project`, `accessId` and `accessKey` can also be passed through `Properties`. 

For example:
    
    Properties config = new Properties();
    config.put("access_id", "...");
    config.put("access_key", "...");
    config.put("project_name", "...");
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
 

## Data Type

| ODPS Type   | Java Type   | JDBC Interface               | JDBC            |  
| :-------: | :-------- | :-------------------- | :-----------: |
| BIGINT      | Long         | int, short, long              | BIGINT        |
| DOUBLE      | Double       | double, float                 | DOUBLE         |
| BOOLEAN     | Boolean     | boolean                        | BOOLEAN       |
| DATETIME    | util.Date    | sql.Date, sql.Time, sql.Timestamp    | TIMESTAMP     |
| STRING      | byte[]       | String                        | VARCHAR       |
| DECIMAL     | math.BigDecimal  | math.BigDecimal       | DECIMAL        |

The data of ODPS can be accessed by the getters of `ResultSet` like `getInt()`, `getTime()`, etc.


### Type Conversion

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


## JDBC Comliance RoadMap


### DataSource 

TODO

### Driver

Fully implented

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


## Integration with SQuirrel SQL Client 


Fist download and install SQuirrel SQL Client from its [website](http://squirrel-sql.sourceforge.net/).

### Driver registeration

1. Select 'Drivers -> New Driver...' to register ODPS's JDBC driver.
2. Enter the driver name and example URL:
  * Name: ODPS
  * Example URL: `jdbc:odps://<endpoint>@<project>`
3. Select 'Extra Class Path -> Add' to add the`odps-jdbc-*-standalone.jar`.
4. Select 'List Drivers'. From the 'Class Name' input box select the ODPS JDBC driver: `com.aliyun.odps.jdbc.OdpsDriver`.
5. Click 'OK' to complete the driver registration. 

### Alias connection

1. Select 'Aliases -> Add Alias...' to create a connection alias to your ODPS instance.
2. Give the connection alias a name in the 'Name' input box.
3. Select the ODPS driver from the 'Driver' drop-down.
4. Modify the example URL as needed to point to your ODPS instance.
5. Enter 'User Name' and 'Password' and click 'OK' to save the connection alias.
6. Double-click the ODPS alias and click 'Connect'.



