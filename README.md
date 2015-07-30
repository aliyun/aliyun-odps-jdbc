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
| DATETIME    | util.Date    | sql.Date/Time/Timestamp    | DATE           |
| STRING      | byte[]       | String                        | VARCHAR       |
| DECIMAL     | math.BigDecimal  | math.BigDecimal       | DECIMAL        |

The data value can be accessed by the getters of `ResultSet` like `getInt()`, `getTime()`, etc.

The implicit type conversion follows the rule:


| ODPS        | BIGINT | DOUBLE | BOOLEAN | DATETIME | STRING | DECIMAL |
| :--------: | :----: | :-----: | :-----: |:-------: |:-----: |:------: |
| boolean    | Y |   | Y |   | Y |   |
| byte       | Y |   |   |   |   |   |
| int        | Y |   |   |   | Y |   |
| short      | Y |   |   |   | Y |   |
| long       | Y |   |   |   | Y |   |
| double     |   | Y |   |   | Y |   |
| float      |   | Y |   |   | Y |   |
| BigDecial  |   |   |   |   | Y | Y |
| String     | Y | Y | Y | Y | Y | Y |
| Date       |   |   |   | Y | Y |   |
| Time       |   |   |   | Y | Y |   |
| Timestamp  |   |   |   | Y | Y |   |


## Functionality of ResultSet


### ResultSet Types

* TYPE_FORWARD_ONLY
* TYPE_SCROLL_INSENSITIVE
  * absolute
  * afterLast
  * beforeFirst Y
  * first
  * isAfterLast
  * IsBeforeFirst Y
  * isFirst
  * isLast
  * last
  * previous
  * relative
* TYPE_SCROLL_SENSITIVE (Not supported)

### ResultSet Concurrency

* CONCUR_READ_ONLY
* CONCUR_UPDATABLE (Not supported)

### ResultSet Holdablity

Not supported
