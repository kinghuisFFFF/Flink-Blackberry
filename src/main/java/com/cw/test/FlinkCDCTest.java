package com.cw.test;


import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class FlinkCDCTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("mydb85") //指定监控的哪台服务器（MySQL安装的位置）
                .port(21020) //MySQL连接的端口号
                .username("r_tom") //用户
                .password("clOloXJEtg3WlydR")//密码
                .databaseList("db_kafka2") //list：可以监控多个库
                .debeziumProperties(getDebeziumProperties())
//                .tableList("db_kafka2.test01") // set captured table
//                .tableList("db_kafka2.products") // set captured table,必须指定
                .tableList("db_kafka2.*") // set captured table,必须指定
                .deserializer(new JsonDebeziumDeserializationSchema()) // 反序列化 binglog转换为json字符串
                .startupOptions(StartupOptions.initial())//从哪开始
                .build();
        DataStreamSource<String> mysqlSourceDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MysqlSource");
        mysqlSourceDS.print();
        env.execute();
    }

    private static Properties getDebeziumProperties(){
        Properties properties = new Properties();
        properties.setProperty("converters", "dateConverters");
        //根据类在那个包下面修改
//        com.cw.utils.MySqlDateTimeConverter
        properties.setProperty("dateConverters.type", "com.cw.utils.MySqlDateTimeConverter");
        properties.setProperty("dateConverters.format.date", "yyyy-MM-dd");
        properties.setProperty("dateConverters.format.time", "HH:mm:ss");
        properties.setProperty("dateConverters.format.datetime", "yyyy-MM-dd HH:mm:ss");
        properties.setProperty("dateConverters.format.timestamp", "yyyy-MM-dd HH:mm:ss");
        properties.setProperty("dateConverters.format.timestamp.zone", "UTC+8");
        properties.setProperty("debezium.snapshot.locking.mode","none"); //全局读写锁，可能会影响在线业务，跳过锁设置
        properties.setProperty("include.schema.changes", "true");
        properties.setProperty("bigint.unsigned.handling.mode","long");
        properties.setProperty("decimal.handling.mode","double");
        properties.setProperty("scan.incremental.snapshot.chunk.key-column","id");
        return properties;
    }

}

