package com.cw.app.dws;


import cn.hutool.core.util.XmlUtil;
import com.cw.bean.GCPower;
import com.cw.utils.MyKafkaUtil;
import com.cw.utils.ReadXmlUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.w3c.dom.Document;


//数据流: Web/App -> Nginx -> SpringBoot -> Mysql -> FlinkApp -> Kafka(ods) -> FlinkApp -> Kafka(dwd/dim) -> FlinkApp -> Kafka(dwm) -> FlinkApp -> Clickhouse(dws)
//进程:           MockDB                 ->Mysql -> FlinkCDCApp -> Kafka(ZK) -> BaseDBApp -> Kafka(Phoenix)  -> OrderWideApp2 -> Kafka -> ProvinceStatsSqlApp  -> Clickhouse

public class PowerSqlApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  //生产环境应该设置为Kafka主题的分区数

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.Flink-CDC将读取binlog的位置信息以状态的方式保存在CK,如果想要做到断点续传,需要从Checkpoint或者Savepoint启动程序
        //2.1 开启Checkpoint,每隔5秒钟做一次CK
//        env.enableCheckpointing(5000L);
//        //2.2 指定CK的一致性语义
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        //2.3 设置任务关闭的时候保留最后一次CK数据
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        //2.4 指定从CK自动重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));
//        //2.5 设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flinkCDC"));
//        //2.6 设置访问HDFS的用户名
//        System.setProperty("HADOOP_USER_NAME", "cw");

        // todo 2.使用DDL的方式创建动态表, 提取事件时间生成WaterMark
        String groupId = "power_g0001";
        String powerWideTopic = "dwm_gx_power_02";

//        {"addActivePower":1151.37,"devGroup":"末并工序","htime":"2022-02-05","id":"27773a8c69e24450b575604f5c939cbf","producerId":"2162304858206502921","producerName":"德永盛","wm_htime":1688634602816}
        String sql1 = "create table power_wide(  " +
                " id String,  " +
                " producerId String,  " +
                " producerName String,  " +
                " wm_htime bigint,  " +
                " devGroup String,  " +
                " htime String,  " +
                " addActivePower decimal,  " +
                " rt AS TO_TIMESTAMP_LTZ(wm_htime, 3),  " +
                " watermark for rt as rt - interval '2' second  " +
                " )with (" + MyKafkaUtil.getKafkaDDL2(powerWideTopic, groupId) + ")";

        String sql2 = "" +
                "select " +
                "   DATE_FORMAT(TUMBLE_START(rt, interval '10' second), 'yyyy-MM-dd HH:mm:ss') stt, " +
                "   DATE_FORMAT(TUMBLE_END(rt, interval '10' second), 'yyyy-MM-dd HH:mm:ss') edt, " +
                "   province_id, " +
                "   province_name, " +
                "   province_area_code, " +
                "   province_iso_code, " +
                "   province_3166_2_code, " +
                "   count(distinct order_id) order_count, " +
                "   sum(split_total_amount) order_amount, " +
                "   UNIX_TIMESTAMP()*1000 ts " +
                "from order_wide " +
                "group by " +
                "   province_id, " +
                "   province_name, " +
                "   province_area_code, " +
                "   province_iso_code, " +
                "   province_3166_2_code, " +
                "   TUMBLE(rt, interval '10' second)";

        String sql3 = "select    DATE_FORMAT(TUMBLE_START(rt, interval '10' second), 'yyyy-MM-dd HH:mm:ss') stt,    DATE_FORMAT(TUMBLE_END(rt, interval '10' second), 'yyyy-MM-dd HH:mm:ss') edt,    producerId,producerName,htime,    count(distinct producerId) gc_count,    sum(addActivePower) gx_power,    UNIX_TIMESTAMP()*1000 ts from power_wide group by    producerId,producerName,htime    TUMBLE(rt, interval '10' second)";

        Document document = XmlUtil.readXML("sql-map/ods/SQL_Power.xml");
        String sql4 = ReadXmlUtils.getTagValueByPath(document, "//sqls/transform/sql03");

        tableEnv.executeSql(sql1);
        //todo 3. 计算工厂每天的能耗 开窗 10秒滚动窗口
        Table resultTable = tableEnv.sqlQuery(sql4);

        //todo 4.将动态表转换为流
        DataStream<GCPower> powerDataStream = tableEnv.toAppendStream(resultTable, GCPower.class);

        //todo 5.将数据写出
        powerDataStream.print(">>>>>>>>>>>>>>");
//        powerDataStream.addSink(ClickHouseUtil.getSink("insert into province_stats_01 values(?,?,?,?,?,?,?,?,?,?)"));

        //todo 6.启动任务
        env.execute("PowerSqlApp");
    }
}
