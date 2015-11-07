# ODPS JDBC (v1.1-SNAPSHOT)

## Installation


1.Use the standalone library:

[odps-jdbc-1.0-public-jar-with-dependencies.jar.zip](https://github.com/aliyun/aliyun-odps-jdbc/raw/master/standalone/odps-jdbc-1.0-public-jar-with-dependencies.jar.zip)

2.Build from source:

```
git clone ....
cd odps-jdbc
mvn install -DskipTests
```

3.Configure and manage the dependency through Maven:

```
<dependency>
  <groupId>com.aliyun.odps</groupId>
  <artifactId>odps-jdbc</artifactId>
  <version>1.0-public</version>
</dependency>
```

## Getting Started

1\. Explictly load the ODPS JDBC driver using `Class.forName()`.
    
For example:
    
    Class.forName("com.aliyun.odps.jdbc.OdpsDriver");


2\. Connect to the ODPS by creating a `Connection` object with the JDBC driver:

    
    Connection conn = DriverManager.getConnection(url, accessId, accessKey);

The ODPS server works with RESTful API, so the url looks like:

    String url = "jdbc:odps:https://your.endpoint.domain/subdomain?project=PROJECT_NAME&charset=UTF-8"


The connection properties can also be passed through `Properties`. 

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

For example:
    
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




## How to Do Test？


1.Copy out a configuration file:

```
cp ./src/test/resources/conf.properties.example ./src/test/resources/conf.properties
```

2.Fill in your connection strings:

```
access_id=...
access_key=...
end_point=...
project_name=...
logview_host=...
character_set=UTF-8
```

3.Run maven test command (or just test it in IntelliJ IDEA):

```
mvn test
```

## Authors && Contributors

- [Cheng Yichao](https://github.com/onesuper)
- [Li Ruibo](https://github.com/lyman)
- [Wen Shaojin](https://github.com/wenshao)

## License

licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html)

