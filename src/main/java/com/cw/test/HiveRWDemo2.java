package com.cw.test;

import com.cw.bean.MyGeneratorFunction2;
import com.cw.bean.WaterSensor;
import com.cw.utils.FilnkUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.time.Duration;

/**
 * @Title:
 * @BelongProjecet Flink-Blackberry
 * @BelongPackage com.cw.test
 * @Description:
 * @Copyright time company - Powered By 研发一部
 * @Author: cw
 * @Date: 2023/6/26 20:50
 * @Version V1.0
 */
public class HiveRWDemo2 {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME","root");

        StreamExecutionEnvironment env = FilnkUtils.getStreamExecutionEnvironmentDev();
        env.setParallelism(1);
        env.enableCheckpointing(5000L);


        DataGeneratorSource dataGeneratorSource = new DataGeneratorSource(new MyGeneratorFunction2(), Long.MAX_VALUE, Types.POJO(WaterSensor.class));
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.fromSource(dataGeneratorSource, WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((element, ts) -> element.getTs() * 1000L), "data-generator")
                // 指定返回类型
                .returns(new TypeHint<WaterSensor>() {
                });
        sensorDS.print();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(10));
        tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);

        // TODO 1. 流转表
        Table sensorTable = tableEnv.fromDataStream(sensorDS);
        tableEnv.createTemporaryView("sensor", sensorTable);

        String name = "myhive";
        String defaultDatabase = "ods";
        String hiveConfDir = "D:\\ws\\GitHub\\Flink-Blackberry\\src\\main\\resources";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        StreamStatementSet statementSet = tableEnv.createStatementSet();
        tableEnv.registerCatalog("myhive", hive);

// set the HiveCatalog as the current catalog of the session
//        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tableEnv.useCatalog(name);
        tableEnv.useDatabase(defaultDatabase);

//        设置读取hive的并行度
        Configuration cfg = tableEnv.getConfig().getConfiguration();
        cfg.setString("table.exec.hive.infer-source-parallelism", "false");
        cfg.setString("table.exec.hive.infer-source-parallelism.max", "15");
        cfg.setString("table.exec.hive.fallback-mapred-reader", "true");


        tableEnv.createTemporaryView("fs_table_source.View", sensorDS);
//// 写入hive的t1表
        tableEnv.executeSql("insert into t1 select * from fs_table_source.View");
        tableEnv.executeSql("show databases").print();
        tableEnv.executeSql("show tables").print();
        tableEnv.executeSql("show databases");

// 只要代码中调用了 DataStreamAPI，就需要 execute，否则不需要
        env.execute();


    }
}
