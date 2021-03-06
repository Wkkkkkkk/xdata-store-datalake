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
        // ??? 60s ????????? checkpoint
        env.enableCheckpointing();
        // checkpoint ??????????????? EXACTLY_ONCE?????????????????????
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // ?????? checkpoint ???????????????????????? 1 s???????????? 0???????????????????????? checkpoint
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        // checkpoint ????????? 60s ??????????????????????????????????????? 10 ??????
        env.getCheckpointConfig().setCheckpointTimeout(600000);
        // ????????????????????????????????? checkpoint
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // ???????????? checkpoint ?????? 3 ???
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        // ??? Flink ??????????????????????????????????????? checkpoint ??????
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        // ??????????????? Savepoint ????????????????????? Checkpoint ?????????
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);
        // ??????????????????????????????????????? checkpoint??????????????????
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        return env;
    }
}
