package com.mirana.project.gmalllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mirana.project.constants.GmallConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
//TODO @RestController = @Controller + @ResponseBody
public class LoggerController {
    //Logger logger= LoggerFactory.getLogger(LoggerController.class);
    @Autowired //自动注入
    private KafkaTemplate kafkaTemplate;

    @RequestMapping("test1")//TODO test1一个方法
    //TODO @ResponseBody //点击网页时返回一个页面
    public String test1(){
        System.out.println("aaaa");
        return "success";
    }
    @RequestMapping("test2")
    public String test2(@RequestParam("name") String nn,
                        @RequestParam("age") String age){
        System.out.println(nn+":"+age);
        return "success";
    }
    //TODO http://localhost:8080/test2?name=zhangsan&age=18
    //TODO 启动GmallLoggerApplication  这个网页被刷新的话   会打印nn:age


    //测试JsonMocker 网页请求的写入
    @RequestMapping("log")
    public String getlog(@RequestParam("logString") String logString){
        //System.out.println(logString);

        //TODO 0.添加时间戳
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts",System.currentTimeMillis());

        //TODO 1. 将数据落盘
        //logger.info();
        log.info(jsonObject.toString());

        //TODO 2.将数据写入Kafka
        if ("startup".equals(jsonObject.getString("type"))){
            kafkaTemplate.send(GmallConstant.GMALL_STARTUP,jsonObject.toString());
        }else {
            kafkaTemplate.send(GmallConstant.GMALL_EVENT,jsonObject.toString());
        }
        return "success";
    }
}
