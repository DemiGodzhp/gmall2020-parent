package com.atguigu.gmall.publisher.controller;

import com.atguigu.gmall.publisher.service.ClickHouseService;
import com.atguigu.gmall.publisher.service.ESService;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author Demigod_zhang
 * @create 2020-10-25 22:24
 */
@RestController
public class PublisherController {
    @Autowired
    ESService esService;

    @Autowired
    ClickHouseService clickHouseService;

    @RequestMapping("/realtime-total")
    public Object realtimeTotal(@RequestParam("date") String dt){

        List<Map<String,Object>> rsList = new ArrayList<Map<String,Object>>();
        Map<String,Object> dauMap = new HashMap<>();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        Long dauTotal = esService.getDauTotal(dt);
        if(dauTotal == null){
            dauMap.put("value",0L);
        }else{
            dauMap.put("value",dauTotal);
        }
        rsList.add(dauMap);


        Map<String,Object> midMap = new HashMap<>();
        midMap.put("id","new_mid");
        midMap.put("name","新增设备");
        midMap.put("value",666);
        rsList.add(midMap);

        Map<String,Object> orderAmountMap = new HashMap<>();
        orderAmountMap.put("id","order_amount");
        orderAmountMap.put("name","新增交易额");
        orderAmountMap.put("value",clickHouseService.getOrderAmountTocal(dt));
        rsList.add(orderAmountMap);

        return rsList;
    }


    @RequestMapping("/realtime-hour")
    public Object realtimeHour(@RequestParam("id") String id,@RequestParam("date") String dt){
        if("dau".equals(id)){
            Map<String,Map<String,Long>> rsMap = new HashMap<>();

            Map<String,Long> tdMap = esService.getDauHour(dt);
            rsMap.put("today",tdMap);


            String yd = getYd(dt);
            Map<String,Long> ydMap = esService.getDauHour(yd);
            rsMap.put("yesterday",ydMap);
            return rsMap;
        }else if("order_amount".equals(id)){
            Map<String,Map<String,BigDecimal>> amoutMap = new HashMap<>();

            Map<String, BigDecimal> tdMap = clickHouseService.getOrderAmountHour(dt);
            amoutMap.put("today",tdMap);


            String yd = getYd(dt);
            Map<String,BigDecimal> ydMap = clickHouseService.getOrderAmountHour(yd);
            amoutMap.put("yesterday",ydMap);
            return amoutMap;
        }else{
            return null;
        }
    }

    private  String getYd(String td){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String yd = null;
        try {
            Date tdDate = dateFormat.parse(td);
            Date ydDate = DateUtils.addDays(tdDate, -1);
            yd = dateFormat.format(ydDate);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("日期格式转变失败");
        }
        return yd;
    }
}
