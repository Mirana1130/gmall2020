package com.mirana.project.constants;

//TODO 封装业务主题名信息，以供后面使用，方便统一修改和调用
public class GmallConstant {
    //启动日志主题
    public static final String GMALL_STARTUP = "TOPIC_START";
    //事件日志主题
    public static final String GMALL_EVENT = "TOPIC_EVENT";

    //订单明细表日志主题
    public static final String GMALL_ORDER_DETAIL = "TOPIC_ORDER_DETAIL";
    //用户信息表日志主题
    public static final String GMALL_USER_INFO = "TOPIC_USER_INFO";

    //订单表日志主题
    public static final String GMALL_ORDER_INFO = "TOPIC_ORDER_INFO";
    //预警日志ES索引前缀
    public static final String GMALL_ALERT_INFO_PREFIX="gmall_coupon_alert";

    //销售明细ES索引前缀
    public static final String GMALL_SALE_DETAIL_ES_PREFIX="gmall2020_sale_detail";


}
