/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;


public class Benchmark {

  public static void run(Statement stmt, int fetchSize) {
    try {
      String sql = "select * from save_private_ryan limit 1000000";
      ResultSet rs = stmt.executeQuery(sql);
      long elpasedTime;
      long start = System.currentTimeMillis();
      {
        int index = 0;
        while (rs.next()) {
          assert ((index + 1) == rs.getRow());
          rs.getInt(1);
          index++;
        }
      }
      long end = System.currentTimeMillis();
      elpasedTime = end - start;

      System.out.printf("step\t%d\tmillis\t%d\n", fetchSize, elpasedTime);
      rs.close();
      stmt.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    try {
      Properties odpsConfig = new Properties();
      FileInputStream file;
      file = new FileInputStream("conf.properties");
      odpsConfig.load(file);

      String endpoint = odpsConfig.getProperty("end_point");
      String project = odpsConfig.getProperty("project_name");
      String user = odpsConfig.getProperty("access_id");
      String password = odpsConfig.getProperty("access_key");
      String logview = odpsConfig.getProperty("logview_host");


      String url = String.format("jdbc:odps:%s?project=%s;logview=%s", endpoint, project, logview);

      Class.forName("com.aliyun.odps.jdbc.OdpsDriver");

      Connection conn = DriverManager.getConnection(url, user, password);

      Statement stmt = conn.createStatement();

      int fetchSize = Integer.parseInt(args[0]);
      run(stmt, fetchSize);

    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (SQLException e) {
      e.printStackTrace();
    } catch(ClassNotFoundException e) {
      e.printStackTrace();
    }
  }
}
