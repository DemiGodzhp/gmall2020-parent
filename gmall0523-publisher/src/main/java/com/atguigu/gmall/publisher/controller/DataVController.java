package com.atguigu.gmall.publisher.controller;

import com.atguigu.gmall.publisher.service.MySQLService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Demigod_zhang
 * @create 2020-11-03 19:49
 */
@RestController
public class DataVController {

    @Autowired
    MySQLService mysqlService;

    @GetMapping("/trademark-sum")
    public Object trademarkSum(@RequestParam("start_date") String startDate,
                               @RequestParam("end_date") String endDate,
                               @RequestParam("topN") int topN){

        List<Map> trademardSum = mysqlService.getTrademardStat(startDate,endDate,topN);
        //根据DataV图形数据要求进行调整，  x :品牌 ,y 金额， s 1
        List<Map> datavList=new ArrayList<>();
        for (Map trademardSumMap : trademardSum) {
            Map  map = new HashMap<>();
            map.put("x",trademardSumMap.get("trademark_name"));
            map.put("y",trademardSumMap.get("amount"));
            map.put("s",1);
            datavList.add(map);
        }
        return datavList;

    }


}
