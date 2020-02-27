package com.pep.common;

import com.alibaba.fastjson.JSON;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * 描述:
 *
 * @author zhangfz
 * @create 2019-10-25 16:18
 */
public class MappingRuleInit {
    public static MappingRuleConfig mappingRuleConfig;

    public static void init(String mappingRule) {
        InputStream inputStream = MappingRuleConfig.class.getResourceAsStream(mappingRule);
        mappingRuleConfig = JSON.parseObject(inputStream2String(inputStream), MappingRuleConfig.class);
    }

    public static String generateSQL(MappingRuleConfig rule, String tmpTableName) {
        Map<String, String> mapping = rule.getMapping();
        StringBuffer sqlBuf = new StringBuffer("insert overwrite table ods.ods_product_user partition(put_date) ");
        sqlBuf.append("select ");
        for (Map.Entry<String, String> entry : mapping.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            sqlBuf.append(("".equals(value) ? "''" : value) + " as " + key + ", ");
        }
        String sql = sqlBuf.substring(0, sqlBuf.length() - 2) + (" from " + tmpTableName);
        return sql;
    }

    public static String inputStream2String(InputStream inputStream) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int i = -1;
        try {
            while ((i = inputStream.read()) != -1) {
                baos.write(i);
            }
            return baos.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }
}
