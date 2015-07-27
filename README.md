# JDBC Driver for ODPS


## Quickstart

### Setup a connection

An ODPS server provide service via RESTFULL API, so there are three kinds of `ODPS_JDBC_URL`
which are acceptable: 

* `jdbc:odps:http://<domain>/<subdomain>`
* `jdbc:odps:https://<domain>/<subdomain>`

If the user does not specify the protocal, an "https" protocal will be used: 

* `jdbc:odps://<domain>/<subdomain>`

Other information is configured through a `java.util.Properties`. For each ODPS connection,
a `project_name` is also required.

    Properties info = new Properties();
    info.put("access_id", "...");
    info.put("access_key", "...");
    info.put("project_name", "...");
       
A JDBC connection is built through the following API call:
    
    Connection conn = driver.connect("<ODPS_JDBC_URL>", info);


Then the client code can manipulate the ODPS database with the JDBC APIs, like:    
    
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("sql query");
    while (rs.next()) {
        ...
    }
 

## Data Type

### Representation and Interface


| ODPS        | Java SDK Interface    | JDBC Interface \[1\]               | JDBC            |  
| :-------: | :--------------- | :-------------------------- | :-----------: |
| BIGINT      | java.lang.Long        | `int`, `short`, `long`                | BIGINT        |
| DOUBLE      | java.lang.Double      | `double`, `float`                     | DOUBLE         |
| BOOLEAN     | java.lang.Boolean    | `boolean`                              | BOOLEAN       |
| DATETIME    | java.util.Date        | `Date`, `Time`, `Timestamp` \[2\] | DATE           |
| STRING      | java.lang.String      | `String`                              | VARCHAR       |
| DECIMAL     | java.math.BigDecimal | `BigDecimal`                        | DECIMAL        |

\[1\] The data value can be accessed by the getters of ResultSet like `getInt()`, `getTime()`, etc.

\[2\] `Date`, `Time`, and `Timestamp` stands for `java.sql.Date`, `java.sql.Time`, and `Timestamp` respectively.


### Casting


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
