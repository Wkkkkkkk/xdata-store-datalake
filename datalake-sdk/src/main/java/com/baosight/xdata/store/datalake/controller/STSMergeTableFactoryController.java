package com.baosight.xdata.store.datalake.controller;

import com.baosight.xdata.store.datalake.controller.service.STSMergeTableFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RestController;

/**
 * @ClassName STSMergeTableFactoryController
 * @Description: TODO
 * @Author wuke
 * @Date 2022-03-09 16:54
 * @Copyright: Copyright (c) 2021
 * @Version 1.0
 **/
@RestController
public class STSMergeTableFactoryController {
    @Autowired
    STSMergeTableFactory stsMergeTableFactory;
    public void sest() {
    }

}
