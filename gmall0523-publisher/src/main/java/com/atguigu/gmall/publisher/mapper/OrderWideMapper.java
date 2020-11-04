package com.atguigu.gmall.publisher.mapper;



import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * @author Demigod_zhang
 * @create 2020-11-02 20:59
 */

public interface OrderWideMapper {

    BigDecimal selectOrderAmountTotal(String date);

    //获取指定日期的分时交易额
    List<Map> selectOrderAmountHour(String date);
}
