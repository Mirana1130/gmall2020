package com.mirana.project.gmallpublisher.controller;


import com.alibaba.fastjson.JSONObject;
import com.mirana.project.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@RestController
public class PublisherController {
    @Autowired
    private PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String getTotal(@RequestParam("date") String date){
     //1.定义集合，用于存放结果数据
     ArrayList<Map> result=  new ArrayList<>();
    //2.查询日活总数
        Integer dauTotal = publisherService.getDauTotal(date);
        //TODO GMV
        Double orderAmount = publisherService.getOrderAmount(date);

        //3.创建日活map
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",dauTotal);
      //4.创建新增用户的Map
        HashMap<String, Object> newMidMap = new HashMap<>();
        newMidMap.put("id","new_mid");
        newMidMap.put("name","新增设备");
        newMidMap.put("value",233);
        //TODO GMV
        HashMap<String, Object> gmvMap = new HashMap<>();
        gmvMap.put("id","order_amount");
        gmvMap.put("name","新增交易额");
        gmvMap.put("value",orderAmount);

        //5.将日活及新增设备Map放入集合
        result.add(dauMap);
        result.add(newMidMap);
        //TODO
        result.add(gmvMap);
      //6.将集合转换为JSON字符串返回
        return JSONObject.toJSONString(result);
    }
    @RequestMapping("realtime-hours")
    public String getTotal(@RequestParam("id") String id,
                          @RequestParam("date") String date){
        //1.创建Map用于存放结果数据
        HashMap<String, Map<Integer, Long>> resultMap = new HashMap<>();
        //2.获取昨天的日期
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();
        //3.声明存放昨天和今天数据的Map
        Map Map1 = null;
        Map Map2 = null;
        if ("dau".equals(id)) {
            //获取当天,昨天的日活分时数据
            Map1= publisherService.getDauTotalHourMap(date);
            Map2= publisherService.getDauTotalHourMap(yesterday);
        }else if ("order_amount".equals(id)){
            Map1=publisherService.getOrderAmountHour(date);
            Map2= publisherService.getOrderAmountHour(yesterday);
        }
        //4.将两个Map放入result
        resultMap.put("yesterday",Map2);
        resultMap.put("today",Map1);
        //5.返回结果
        return JSONObject.toJSONString(resultMap);
    }
    @RequestMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date") String date,
                                @RequestParam("startpage") int startpage,
                                @RequestParam("size") int size,
                                @RequestParam("keyword") String keyword) throws IOException {

        return publisherService.getSaleDetail(date, startpage, size, keyword);
    }

//    @RequestMapping("sale_detail")
//    public String getSaleDetail() throws IOException {
//        return "您无查询参数";
//    }


}