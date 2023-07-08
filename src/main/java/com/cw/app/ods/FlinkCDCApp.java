package com.cw.app.ods;


import com.cw.app.func.MyDeserializationSchemaFunction;
import com.cw.utils.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 *
 */
public class FlinkCDCApp {
    static String simpleClassName = FlinkCDCApp.class.getSimpleName();

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");

//        String simpleClassName = Thread.currentThread().getStackTrace()[1].getClassName();
//        String simpleClassName = Thread.currentThread().getStackTrace()[1].getClassName();
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamExecutionEnvironment env = FlinkUtils.getStreamExecutionEnvironmentByDev();
        env.setParallelism(1);

        //1.2 设置CK&状态后端 生产环境必须有这些代码
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));
//        hdfs:///hadoop31:9000/flink/checkpoints
        env.setStateBackend(new FsStateBackend("hdfs://hadoop31:9000/flink/checkpoints"));
////        hdfs://edh/flink/job2/ck/
//        env.setStateBackend(new FsStateBackend("hdfs://edh/flink/job3/ck/"));

//        Hsapi/Hsapi-2016
        //2.使用DataStream方式的CDC读取MySQL变化数据(只读取最新的数据)
        MySqlSource<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("mydb85") //指定监控的哪台服务器（MySQL安装的位置）
                .port(21020) //MySQL连接的端口号
                .username("r_tom") //用户
                .password("clOloXJEtg3WlydR")//密码
                .databaseList("db_kafka2") //list：可以监控多个库
                .tableList("db_kafka2.*") // set captured table
                .debeziumProperties(getDebeziumProperties())
//                .deserializer(new StringDebeziumDeserializationSchema())
                .deserializer(new MyDeserializationSchemaFunction())
                .startupOptions(StartupOptions.initial()) // 从头开始消费
//                .startupOptions(StartupOptions.latest()) //从最新位置开始消费
                .build();
        DataStreamSource<String> mysqlDS = env.fromSource(sourceFunction, WatermarkStrategy.noWatermarks(), "MysqlSource");
        //3.将数据写入Kafka
        String topic = "ods_base_db";
        mysqlDS.addSink(MyKafkaUtil.getKafkaSink(topic));
        mysqlDS.print(">>>>>>>");

        //4.启动任务
        env.execute(simpleClassName);
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
//        //scan.incremental.snapshot.chunk.key-column 可以指定某一列作为快照阶段切分分片的切分列。无主键表必填，选择的列必须是非空类型（NOT NULL）。
//                //有主键的表为选填，仅支持从主键中选择一列。
        properties.setProperty("scan.incremental.snapshot.chunk.key-column","id");
        return properties;
    }

}
