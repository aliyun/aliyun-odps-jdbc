package com.aliyun.odps.jdbc;

import java.lang.reflect.Field;
import java.util.Properties;

import junit.framework.TestCase;

import com.aliyun.odps.cli.Main;
import com.aliyun.odps.cli.Session;
import com.aliyun.odps.cli.sql.SQLCommand;
import com.aliyun.odps.jdbc.impl.OdpsConnection;

public class ExecUpdateTest extends TestCase {

    public void test_update() throws Exception {
        OdpsDriver driver = OdpsDriver.instance;
        
        Properties info = new Properties();
        info.put("access_id", BVTConf.getAccessId());
        info.put("access_key", BVTConf.getAccessKey());
        OdpsConnection conn = (OdpsConnection) driver.connect("jdbc:odps:service-corp.odps.aliyun-inc.com/secdw_dev", info);
        
        String sql = "insert into table test_wenshao_0622 select count(*) from dual;";
//        Statement stmt = conn.createStatement();
//        stmt.executeUpdate(sql);
//        stmt.close();
//        conn.close();
        
        sql = "select * from dual;";
//        SQLCommand cmd = new SQLCommand();
//        Session session = new Session();
//        {
//            Field field = Session.class.getDeclaredField("odps");
//            field.setAccessible(true);
//            field.set(session, conn.getOdps());
//        }
//        session.setDefaultProject("secdw_dev");
//        cmd.exec(sql, session);
        
        {
            String confPath = "/work/install/odps/conf/odps_config.ini";
            String[] args = new String[] {"-c", confPath};
            Main.main(args);
            
            Thread.sleep(1000 * 10000);
        }
   
    }
}
