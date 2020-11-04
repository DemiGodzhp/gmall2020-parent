package com.atguigu.gmall.publisher.service;

//import com.sun.javafx.collections.MappingChange;

import com.sun.javafx.collections.MappingChange;

import java.util.Map;

/**
 * @author Demigod_zhang
 * @create 2020-10-25 22:04
 */
public interface ESService {
    public Long getDauTotal(String date);
    public Map<String,Long> getDauHour(String date);
}
