package com.aliyun.openservices.odps.jdbc;

import java.util.Map;

import com.aliyun.openservices.odps.tables.RecordReader;

public class OdpsJDBCQueryData {
    private RecordReader reader;
    private Map<String, String> tableMeta;
    public OdpsJDBCQueryData(RecordReader reader, Map<String, String> tableMeta) {
        super();
        this.reader = reader;
        this.tableMeta = tableMeta;
    }
    public RecordReader getReader() {
        return reader;
    }
    public Map<String, String> getTableMeta() {
        return tableMeta;
    }
    

}
