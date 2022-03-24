package com.baosight.xinsight.datalake.entity;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.apache.flink.streaming.api.environment.RemoteStreamEnvironment;
import org.hamcrest.collection.IsArrayWithSize;

@Data
@Getter
@Setter
public class DLAInput {
    private String tableName;
    private String tableSource;
    private String tableDDL;
    private String jobExecute;
    private String flinkHost = "127.0.0.1";
    private int flinkPort = 8081;
    private String jarListUrl;
    private String HDFSUser = "hive";
    private RemoteStreamEnvironment renv;

    public void setFlink(String flinkHost, int flinkPort, String HDFSUser) {
        this.flinkHost = flinkHost;
        this.flinkPort = flinkPort;
        this.HDFSUser = HDFSUser;
    }

    public void setFlink(String flinkHost, int flinkPort, String HDFSUser, String jarListUrl) {
        this.flinkHost = flinkHost;
        this.flinkPort = flinkPort;
        this.HDFSUser = HDFSUser;
        this.jarListUrl = jarListUrl;
    }

    public void setDLSTable(String tableName, String tableDDL) {
        this.tableName = tableName;
        this.tableDDL = tableDDL;
        this.tableSource = "";
        this.jobExecute = "";

    }
}
