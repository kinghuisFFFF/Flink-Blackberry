package com.cw.test;
 
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
 
public class FlinkCDC {
    public static void main(String[] args) throws Exception {
 
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 设置任务关闭时候保留最后一次checkpoint 的数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 指定ck 的自动重启策略
        env.setStateBackend(new FsStateBackend("hdfs://192.168.1.161:8020/cdc2.0-test/ck"));
        // 设置hdfs 的访问用户名
        System.setProperty("HADOOP_USER_NAME","hdfs");
 
        DebeziumSourceFunction<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("192.168.1.180")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("test")
                .tableList("test.Flink_iceberg")
                .deserializer(new StringDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
        DataStreamSource<String> dataStreamSource = env.addSource(mySqlSource);
        dataStreamSource.print();
        env.execute();
 
 
    }
}