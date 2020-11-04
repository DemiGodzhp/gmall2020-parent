package com.atguigu.gmall.publisher.service;

import java.util.List;
import java.util.Map;

/**
 * @author Demigod_zhang
 * @create 2020-11-03 19:46
 */
public interface MySQLService {

     List<Map> getTrademardStat(String startDate, String endDate, int topN);
}
