package com.baosight.xdata.store.datalake.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @ClassName HelloContriller
 * @Description: 测试
 * @Author wuke
 * @Date 2022-03-09 15:34
 * @Copyright: Copyright (c) 2021
 * @Version 1.0
 **/
@RestController
public class HelloController {
    @GetMapping("/t1")
    public String hello() {
        return "hello world";
    }
}
