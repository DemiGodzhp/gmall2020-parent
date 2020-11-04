package com.atguigu.gmall.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Demigod_zhang
 * @create 2020-10-18 22:45
 */
@RestController
@Slf4j
public class LoggerController {
    @Autowired
    KafkaTemplate kafkaTemplate;
    @RequestMapping("/applog")
    public String applog(@RequestBody String jsonLog){

        log.info(jsonLog);

        JSONObject jsonObject = JSON.parseObject(jsonLog);
        if(jsonObject.getJSONObject("start")!=null){

            kafkaTemplate.send("gmall_start_0523",jsonLog);
        }else{

            kafkaTemplate.send("gmall_event_0523",jsonLog);
        }
        return jsonLog;
    }
}
