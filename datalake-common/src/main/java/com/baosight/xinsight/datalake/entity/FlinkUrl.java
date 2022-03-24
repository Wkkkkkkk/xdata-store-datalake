package com.baosight.xinsight.datalake.entity;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
@Getter
@Setter
public class FlinkUrl {
    private String flinkHost;
    private int flinkPort;
    private String jarListUrl;

    FlinkUrl(String flinkHost, int flinkPort, String jarListUrl) {
        this.flinkHost = flinkHost;
        this.flinkPort = flinkPort;
        this.jarListUrl = jarListUrl;
    }
}
