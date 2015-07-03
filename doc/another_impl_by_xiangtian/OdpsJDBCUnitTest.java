package com.aliyun.openservices.odps.jdbc;

import static org.junit.Assert.*;

import org.junit.Test;

public class OdpsJDBCUnitTest {
    @Test
    public void testGetConnctionUrl(){
        String url="http://service.odps.aliyun-inc.com/api/projects/myproject";
        assertEquals("http://service.odps.aliyun-inc.com/api",getConnctionUrl(url));
        assertEquals("myproject",getProjectName(url));
    }
    
    private String getConnctionUrl(String url){
        if(url!=null&&url.indexOf("/projects/")>1){
            return url.substring(0, url.indexOf("/projects/"));
        }
        return null;
    }
    
    private String getProjectName(String url){
        if(url!=null&&url.indexOf("/projects/")>1){
            return url.substring(url.indexOf("/projects/")+10);
        }
        return null;
    }
}
