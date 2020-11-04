package com.atguigu.gmall.publisher.mapper;

import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * @author Demigod_zhang
 * @create 2020-11-03 19:40
 */
@Service
public interface TrademarkStatMapper {
     List<Map> selectTradeSum(@Param("start_date") String startDate,
                                    @Param("end_date") String endDate,
                                    @Param("topN") int topN);

}
