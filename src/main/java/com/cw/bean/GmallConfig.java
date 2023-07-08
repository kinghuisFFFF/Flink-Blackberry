package com.cw.bean;

public class GmallConfig {

    //Phoenix库名
    public static final String HBASE_SCHEMA = "\"ODS\"";

    //Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

//    jdbc:phoenix:hadoop31:2181
//    jdbc:phoenix:hadoop31,hadoop32,hadoop33:2181
//    jdbc:phoenix:hadoop362,hadoop363,hadoop364:2181
    //Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop31,hadoop32,hadoop33:2181";

    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://bridged-node-220:8123/gmall";

    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

}

