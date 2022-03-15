package com.baosight.xdata.store.datalake.test;

import org.apache.hudi.configuration.FlinkOptions;
import org.junit.jupiter.api.Test;

import static com.baosight.xdata.store.datalake.utils.MergeTableSql.schemaBuilder;


/**
 * @ClassName test
 * @Description: TODO
 * @Author wuke
 * @Date 2022-03-09 12:53
 * @Version 1.0
 **/

public class test {
    @Test
    public void aa() {

        String end = schemaBuilder("t1")
                .field("a1 varchar")
                .partitionField("aaa var")
                .pkField("aaa")
                .pkField("aaa1")
                .option(FlinkOptions.PATH, "aa")
                .option(FlinkOptions.TABLE_TYPE,FlinkOptions.TABLE_TYPE_COPY_ON_WRITE)
                .end();

        System.out.println(end);
    }
}
