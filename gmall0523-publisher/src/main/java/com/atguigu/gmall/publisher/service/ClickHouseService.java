package com.atguigu.gmall.publisher.service;

import java.math.BigDecimal;
import java.util.Map;

/**
 * @author Demigod_zhang
 * @create 2020-11-02 21:07
 */
public interface ClickHouseService {
    BigDecimal getOrderAmountTocal(String date);

    Map<String,BigDecimal> getOrderAmountHour(String date);
}
