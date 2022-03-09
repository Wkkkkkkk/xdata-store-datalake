package com.baosight.xdata.store.datalake;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;

@ServletComponentScan
@SpringBootApplication
public class DatalakeSdkApplication {

    public static void main(String[] args) {
        SpringApplication.run(DatalakeSdkApplication.class, args);
    }

}
