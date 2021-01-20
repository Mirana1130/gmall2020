package com.mirana.project.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface OrderMapper {

    //1 查询当日交易额总数
    public Double selectOrderAmountTotal(String date);

    //2 查询当日交易额分时明细
    public List<Map> selectOrderAmountHourMap(String date);

    //3.GMVOrderAmount
    public Double getOrderAmount(String date);
    //3.GMV分时数据
    public Map getOrderAmountHour(String date);


}
