# JDBC Driver for ODPS


## Installation

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
 









