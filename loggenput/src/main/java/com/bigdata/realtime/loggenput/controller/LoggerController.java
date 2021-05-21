package com.bigdata.realtime.loggenput.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author ydc
 */
@RestController
@Slf4j
public class LoggerController {

    @Autowired
    KafkaTemplate kafkaTemplate;

    @RequestMapping("/applog")
    public String applog(@RequestBody String jsonLog){
        log.info(jsonLog);

        JSONObject logJsonObj = JSON.parseObject(jsonLog);
        if(logJsonObj.getJSONObject("start") == null){
            kafkaTemplate.send("gmall_event",jsonLog);
        }else{
            kafkaTemplate.send("gmall_start",jsonLog);
        }
        return "success";
    }



}
