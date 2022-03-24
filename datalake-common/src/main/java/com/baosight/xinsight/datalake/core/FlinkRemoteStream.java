package com.baosight.xinsight.datalake.core;

import com.baosight.xinsight.datalake.entity.DLAInput;
import com.baosight.xinsight.datalake.entity.DLAOutput;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.RemoteStreamEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

public class FlinkRemoteStream {
    public static DLAOutput DLA(DLAInput dlaInput) throws MalformedURLException {
        DLAOutput dlaOutput = new DLAOutput();
        List<String> jarlist = new ArrayList<>();
        jarlist.add(dlaInput.getJarListUrl() + "\\hudi-flink-bundle_2.11-0.10.1.jar");
        jarlist.add(dlaInput.getJarListUrl() + "\\flink-sql-connector-kafka_2.11-1.13.2.jar");
        jarlist.add(dlaInput.getJarListUrl() + "\\flink-json-1.13.2.jar");
        jarlist.add(dlaInput.getJarListUrl() + "\\hadoop-common-2.6.0-cdh5.16.2.jar");
        String[] jars = jarlist.toArray(new String[0]);
        RemoteStreamEnvironment env = new RemoteStreamEnvironment(
                dlaInput.getFlinkHost(),
                dlaInput.getFlinkPort(),
                jars);
        RemoteStreamEnvironment envconf = envconf(env);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(envconf);
        System.setProperty("HADOOP_USER_NAME", dlaInput.getUser());
        List<URL> dependencyURL = new ArrayList<>();
        for (String jar : jars) {
            String uri = "file://" + jar;
            System.out.println(uri);
            URL url = new URL(uri);
            dependencyURL.add(url);
        }
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        URLClassLoader newLoader = new URLClassLoader(dependencyURL.toArray(new URL[0]), classLoader);
        Thread.currentThread().setContextClassLoader(newLoader);
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("pipeline.name", "hudi_test1");
        tableEnv.executeSql(dlaInput.getTableSource());
        tableEnv.executeSql(dlaInput.getTableDDL());
        TableResult tableResult = tableEnv.executeSql(dlaInput.getJobExecute());
        dlaOutput.setJobResult(tableResult.getJobClient().isPresent());
        return dlaOutput;
    }

    public static  RemoteStreamEnvironment envconf(RemoteStreamEnvironment env) {
        // 每 60s 做一次 checkpoint
        env.enableCheckpointing();
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
        return env;
    }
}
