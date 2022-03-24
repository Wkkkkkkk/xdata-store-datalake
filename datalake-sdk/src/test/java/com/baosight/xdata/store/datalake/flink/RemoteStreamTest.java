package com.baosight.xdata.store.datalake.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.RemoteStreamEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

public class RemoteStreamTest {

    @Test
    public void test() throws MalformedURLException {
        List<String> jarlist = new ArrayList<>();
        //路径应该为类似 /C:/Users/Noel/Documents/xinsight
        String xinsight = System.getenv("XINSIGHT_HOME");
        System.out.println(xinsight);
        jarlist.add("/Users/wuke/maven/repository/org/apache/hudi/hudi-flink-bundle_2.11/0.10.1/hudi-flink-bundle_2.11-0.10.1.jar");
        jarlist.add("/Users/wuke/maven/repository/org/apache/flink/flink-sql-connector-kafka_2.12/1.13.2/flink-sql-connector-kafka_2.12-1.13.2.jar");
        jarlist.add("/Users/wuke/maven/repository/org/apache/flink/flink-json/1.13.2/flink-json-1.13.2.jar");
        jarlist.add("/Users/wuke/Downloads/hadoop-common-2.6.0-cdh5.16.2.jar");
//        jarlist.add("/Users/wuke/Downloads/hadoop-mapreduce-client-core-2.6.0-cdh5.16.2.jar");
//        jarlist.add("/Users/wuke/Downloads/hadoop-mapreduce-client-common-2.6.0-cdh5.16.2.jar");

        String[] jars = jarlist.toArray(new String[0]);
        RemoteStreamEnvironment env = new RemoteStreamEnvironment(
                "cdh3.cloud",
                43562,
                jars);
        env.setParallelism(4);
        // 每 60s 做一次 checkpoint
        env.enableCheckpointing(60000);
        // checkpoint 语义设置为 EXACTLY_ONCE，这是默认语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 两次 checkpoint 的间隔时间至少为 1 s，默认是 0，立即进行下一次 checkpoint
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        // checkpoint 必须在 60s 内结束，否则被丢弃，默认是 10 分钟
        env.getCheckpointConfig().setCheckpointTimeout(600000);
        // 同一时间只能允许有一个 checkpoint
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 最多允许 checkpoint 失败 3 次
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        // 当 Flink 任务取消时，保留外部保存的 checkpoint 信息
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        // 当有较新的 Savepoint 时，作业也会从 Checkpoint 处恢复
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);
        // 允许实验性的功能：非对齐的 checkpoint，以提升性能
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        List<URL> dependencyURL = new ArrayList<>();
        for (String jar : jars) {
            String uri = "file://" + jar;
            System.out.println(uri);
            URL url = new URL(uri);
            dependencyURL.add(url);
        }
        System.out.println("----------------------------------");

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        URLClassLoader newLoader = new URLClassLoader(dependencyURL.toArray(new URL[0]), classLoader);
        Thread.currentThread().setContextClassLoader(newLoader);
        String tb = "hd11";
        String source = "CREATE TABLE k (\n" +
                "   tinyint0 TINYINT\n" +
                "  ,smallint1 SMALLINT\n" +
                "  ,int2 INT\n" +
                "  ,bigint3 BIGINT\n" +
                "  ,float4 FLOAT\n" +
                "  ,double5 DOUBLE  \n" +
                "  ,decimal6 DECIMAL(38,8)\n" +
                "  ,boolean7 BOOLEAN\n" +
                "  ,char8 STRING\n" +
                "  ,varchar9 STRING\n" +
                "  ,string10 STRING\n" +
                "  ,timestamp11 STRING\n" +
                ") WITH (\n" +
                "      'connector' = 'kafka'\n" +
                "    , 'topic' = 'all_type'\n" +
                "    , 'scan.startup.mode' = 'earliest-offset'\n" +
                "    , 'properties.bootstrap.servers' = 'cdh1.cloud:9092'\n" +
                "    , 'properties.group.id' = 'testgroup1' \n" +
                "    , 'value.format' = 'json'\n" +
                "    , 'value.json.fail-on-missing-field' = 'true'\n" +
                "    , 'value.fields-include' = 'ALL'\n" +
                ")";
        String sink = "CREATE TABLE " + tb + "(\n" +
                "    tinyint0 TINYINT \n" +
                "  , smallint1 SMALLINT\n" +
                "  , int2 INT\n" +
                "  , bigint3 BIGINT\n" +
                "  , float4 FLOAT\n" +
                "  , double5 DOUBLE  \n" +
                "  , decimal6 DECIMAL(12,3)\n" +
                "  , boolean7 BOOLEAN\n" +
                "  , char8 CHAR(64) \n" +
                "  , varchar9 VARCHAR(64)\n" +
                "  , string10 STRING\n" +
                "  , timestamp11 TIMESTAMP(3)\n" +
                "  , PRIMARY KEY (char8) NOT ENFORCED   \n" +
                " )\n" +
                " WITH (\n" +
                "     'connector' = 'hudi'\n" +
                "   , 'path' = 'hdfs:///data/hudi/" + tb + "'\n" +
                "   , 'write.precombine.field' = 'timestamp11'\n" +
                "   , 'write.bucket_assign.tasks' = '4'\n" +
                "   , 'write.tasks' = '4'\n" +
                "   , 'hive_sync.enable' = 'true'\n" +
                "   , 'hive_sync.mode' = 'hms'\n" +
                "   , 'hive_sync.metastore.uris' = 'thrift://cdh1.cloud:9083'\n" +
                "   , 'hive_sync.table' = '" + tb + "'\n" +
                "   , 'hive_sync.db' = 'hudi'\n" +
                "   , 'hive_sync.username' = 'hive'\n" +
                "   , 'hive_sync.password' = 'q1w2e3'\n" +
                " )";
        String queryClause = "insert into " + tb + "\n" +
                "select   \n" +
                "      cast(tinyint0 as TINYINT)\n" +
                "    , cast(smallint1 as SMALLINT)\n" +
                "    , cast(int2 as INT)\n" +
                "    , cast(bigint3 as BIGINT)\n" +
                "    , cast(float4 as FLOAT)\n" +
                "    , cast(double5 as DOUBLE)\n" +
                "    , cast(decimal6 as DECIMAL(38,18))\n" +
                "    , cast(boolean7 as BOOLEAN)\n" +
                "    , cast(char8 as CHAR(64))\n" +
                "    , cast(varchar9 as VARCHAR(64))\n" +
                "    , cast(string10 as STRING)\n" +
                "    , cast(timestamp11 as TIMESTAMP(3)) \n" +
                " from  k";
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("pipeline.name", "hudi_test1");
        tableEnv.executeSql(source);
        tableEnv.executeSql(sink);
        TableResult tableResult = tableEnv.executeSql(queryClause);
        System.out.println(tableResult.getJobClient().isPresent());
    }



}
