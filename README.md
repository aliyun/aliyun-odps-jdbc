# JDBC for ODPS


## Quickstart

    Properties info = new Properties();
    info.put("access_id", "...");
    info.put("access_key", "...");
    info.put("project_name", "...");
    
    Connection conn = driver.connect("jdbc:odps://<odps_endpoint>"l, info);
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("sql query");
    while (rs.next()) {
        ...
    }
    

## Data Type

| ODPS        | Java SDK Interface    | JDBC Interface \[1\]            | JDBC              |  
| :-------: | :--------------- | :---------------------- | :-----------: |
| BIGINT      | java.lang.Long        | Int, Short, Long                | BIGINT           |
| DOUBLE      | java.lang.Double      | Dougle, Float                   | DOUBLE           |
| BOOLEAN     | java.lang.Boolean    | Boolean                          | BOOLEAN          |
| DATETIME    | java.util.Date        | Date, Time, Timestamp \[2\]     | DATE              |
| STRING      | java.lang.String      | String                           | VARCHAR          |
| DECIMAL     | java.math.BigDecimal | BigDecimal                      | DECIMAL          |

\[1\] The data value can be accessed by the getters of ResultSet like `getInt()`, `getTime()`, etc.
\[2\] `java.sql.Date`, `java.sql.Time`, and `Timestamp` respectively.
