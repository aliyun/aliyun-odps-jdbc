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

| ODPS        | Java SDK Interface    | JDBC Interface                 | JDBC              |  
| :-------: | :--------------- | :---------------------- | :-----------: |
| BIGINT      | java.lang.Long        | Int, Short, Long                | BIGINT           |
| DOUBLE      | java.lang.Double      | Dougle, Float                   | DOUBLE           |
| BOOLEAN     | java.lang.Boolean    | Boolean                          | BOOLEAN          |
| DATETIME    | java.util.Date        | Date, Time, Timestamp \*      | DATE              |
| STRING      | java.lang.String      | String                           | VARCHAR          |
| DECIAL      | java.math.BigDecimal | BigDecimal                      | DECIMAL          |


\* Date, Time, Timestamp are `java.sql.Date`, `java.sql.Time`, and `Timestamp` respectively.
