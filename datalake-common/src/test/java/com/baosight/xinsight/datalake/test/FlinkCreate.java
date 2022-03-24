package com.baosight.xinsight.datalake.test;

import com.baosight.xinsight.datalake.core.FlinkRemoteStream;
import com.baosight.xinsight.datalake.entity.DLAInput;
import org.junit.Test;

import java.net.MalformedURLException;

public class FlinkCreate {


    @Test
    public void test() throws MalformedURLException {
        DLAInput dlaInput = new DLAInput();
        dlaInput.setFlink("172.16.11.131",3674,"hdfs","D:\\Project\\IdeaProject\\xdata-store-datalake\\datalake-common\\src\\main\\resources\\jar");
        FlinkRemoteStream.DLA(dlaInput);
    }
}

