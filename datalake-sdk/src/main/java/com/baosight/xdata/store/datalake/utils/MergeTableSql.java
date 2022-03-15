package com.baosight.xdata.store.datalake.utils;


import lombok.extern.log4j.Log4j2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName MergeSql
 * @Description: TODO
 * @Author wuke
 * @Date 2022-03-10 13:52
 * @Version 1.0
 **/
@Log4j2
public class MergeTableSql {

    private MergeTableSql() {

    }

    public void aa() {
        log.info("aaa");
    }
//    public static String getCreateHoodieTableDDL(
//            String tableName,
//            List<String> fields,
//            Map<String, String> options,
//            boolean havePartition,
//            String partitionField) {
//        return getCreateHoodieTableDDL(tableName, fields, options, havePartition, "uuid", partitionField);
//    }

    public static String getCreateHoodieTableDDL(
            String tableName,
            List<String> fields,
            Map<String, String> options,
            boolean havePartition,
            String pkField,
            String partitionField) {
        StringBuilder builder = new StringBuilder();
        builder.append("create table ").append(tableName).append("(\n");
        for (String field : fields) {
            builder.append("  ").append(field).append(",\n");
        }
        builder.append("  PRIMARY KEY(").append(pkField).append(") NOT ENFORCED\n")
                .append(")\n");
        if (havePartition) {
            builder.append("PARTITIONED BY (`").append(partitionField).append("`)\n");
        }
        final String connector = options.computeIfAbsent("connector", k -> "hudi");
        builder.append("with (\n"
                + "  'connector' = '").append(connector).append("'");
        options.forEach((k, v) -> builder.append(",\n")
                .append("  '").append(k).append("' = '").append(v).append("'"));
        builder.append("\n)");
        return builder.toString();
    }

    public static schemaBuilder schemaBuilder(String tableName) {
        return new schemaBuilder(tableName);
    }

    public static class schemaBuilder {
        private final Map<String, String> options;
        private final String tableName;
        private List<String> fields = new ArrayList<>();
        private boolean withPartition = true;
        private String pkField = "uuid";
        private String partitionField = "partition";

        public schemaBuilder(String tableName) {
            options = new HashMap<>();
            this.tableName = tableName;
        }

        public schemaBuilder option(ConfigOption<?> option, Object val) {
            this.options.put(option.key(), val.toString());
            return this;
        }

        public schemaBuilder option(String key, Object val) {
            this.options.put(key, val.toString());
            return this;
        }

        public schemaBuilder options(Map<String, String> options) {
            this.options.putAll(options);
            return this;
        }

        public schemaBuilder noPartition() {
            this.withPartition = false;
            return this;
        }

        public schemaBuilder pkField(String pkField) {
            this.pkField = pkField;
            return this;
        }

        public schemaBuilder partitionField(String partitionField) {
            this.partitionField = partitionField;
            return this;
        }

        public schemaBuilder field(String fieldSchema) {
            fields.add(fieldSchema);
            return this;
        }

        public String end() {
            if (this.fields.size() == 0) {
                return "没得字段 宝贝";
            }
            return MergeTableSql.getCreateHoodieTableDDL(this.tableName, this.fields, options,
                    this.withPartition, this.pkField, this.partitionField);
        }
    }

}
