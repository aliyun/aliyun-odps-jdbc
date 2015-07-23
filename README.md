# JDBC for ODPS


## Quickstart


    Properties info = new Properties();
    info.put("access_id", "...");
    info.put("access_key", "...");
    info.put("project_name", "...");
    
    Connection conn = driver.connect("jdbc:odps://<odps_endpoint>", info);
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("sql query");
    while (rs.next()) {
        ...
    }
    

## Data Type

### Representation and Interface

| ODPS        | Java SDK Interface    | JDBC Interface \[1\]            | JDBC              |  
| :-------: | :--------------- | :---------------------- | :-----------: |
| BIGINT      | java.lang.Long        | int, short, long                | BIGINT           |
| DOUBLE      | java.lang.Double      | double, float                   | DOUBLE           |
| BOOLEAN     | java.lang.Boolean    | bool                          | BOOLEAN          |
| DATETIME    | java.util.Date        | Date, Time, Timestamp \[2\]     | DATE              |
| STRING      | java.lang.String      | String                           | VARCHAR          |
| DECIMAL     | java.math.BigDecimal | BigDecimal                      | DECIMAL          |

\[1\] The data value can be accessed by the getters of ResultSet like `getInt()`, `getTime()`, etc.

\[2\] `java.sql.Date`, `java.sql.Time`, and `Timestamp` respectively.


### Casting

| ODPS        | BIGINT | DOUBLE | BOOLEAN | DATETIME | STRING | DECIMAL |
| :-------: | :----: | :----: | :-----: |:-----: |:-----: |:-----: |
| bool         |    Y     |          |    Y      |           |    Y     |          |
| byte         |    Y     |          |           |           |          |          |
| int          |    Y     |          |           |           |     Y    |          |
| short        |   Y      |          |           |           |    Y     |          |
| long         |    Y     |          |           |           |    Y     |          |
| double       |          |    Y     |           |           |    Y     |          |
| float        |          |    Y     |           |           |    Y     |          |
| BigDecial   |          |          |           |           |    Y     |    Y     |
| String      |    Y     |    Y     |     Y     |     Y     |    Y     |    Y     |
| Date        |          |          |           |     Y     |    Y     |          |
| Time        |          |          |           |     Y     |    Y     |          |
| Timestamp  |          |          |           |     Y     |    Y     |          |

