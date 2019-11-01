package com.pep.common;

import com.alibaba.fastjson.JSON;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 描述:
 *
 * @author zhangfz
 * @create 2019-10-25 15:31
 */
public class MappingRuleConfig {
    private String db_user;
    private String db_password;
    private String db_url;
    private String table_name;
    private LinkedHashMap<String,String> mapping;
    private String partitionsByKey;

    public String getDb_user() {
        return db_user;
    }

    public void setDb_user(String db_user) {
        this.db_user = db_user;
    }

    public String getDb_password() {
        return db_password;
    }

    public void setDb_password(String db_password) {
        this.db_password = db_password;
    }

    public String getDb_url() {
        return db_url;
    }

    public void setDb_url(String db_url) {
        this.db_url = db_url;
    }

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        this.table_name = table_name;
    }

    public LinkedHashMap<String, String> getMapping() {
        return mapping;
    }

    public void setMapping(LinkedHashMap<String, String> mapping) {
        this.mapping = mapping;
    }

    public String getPartitionsByKey() {
        return partitionsByKey;
    }

    public void setPartitionsByKey(String partitionsByKey) {
        this.partitionsByKey = partitionsByKey;
    }


}
