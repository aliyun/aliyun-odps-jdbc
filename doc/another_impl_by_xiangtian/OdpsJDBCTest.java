package com.aliyun.openservices.odps.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class OdpsJDBCTest {
    public static final String url = "http://service.odps.aliyun-inc.com/api/projects/wbstest";
    public static final String user = "accessid";
    public static final String password = "accesskey";
    public static final String project = "wbstest";
    public static final String sql = "select * from xxx";  

    public static void main(String args[]) {
        // close apache common logging
        System.setProperty("org.apache.commons.logging.Log",
                "org.apache.commons.logging.impl.NoOpLog");
        try {
            Class.forName("com.aliyun.openservices.odps.jdbc.OdpsJDBCDriver");
            Connection con = DriverManager.getConnection(url, user, password);
            Statement stmt = con.createStatement();
            ResultSet rs=stmt.executeQuery(sql);
            while(rs.next())
            {
                //通过列名获取结果
                System.out.println(rs.getString("key")+" "+rs.getString("value"));
                //通过列号获取结果
                System.out.println(rs.getString(0)+" "+rs.getString(1)); 
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

}
