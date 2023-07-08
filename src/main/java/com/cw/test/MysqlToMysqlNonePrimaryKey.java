package com.cw.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
 
 
public class MysqlToMysqlNonePrimaryKey {
 
    public static void main(String[] args) {
        //1.获取stream的执行环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        //2.创建表执行环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(senv, settings);
 
        String sourceTable = "CREATE TABLE mysql_cdc_source (" +
                "  id INT,\n" +
                "  username STRING,\n" +
                "  password STRING\n" +
                ") WITH (\n" +
                "'connector' = 'mysql-cdc',\n" +
                "'hostname' = 'localhost',\n" +
                "'port' = '3306',\n" +
                "'username' = 'root',\n" +
                "'password' = 'root',\n" +
                "'database-name' = 'test_cdc',\n" +
                "'debezium.snapshot.mode' = 'initial',\n" +
                "'scan.incremental.snapshot.enabled' = 'false',\n" +
                //如果开启增量快照，必须设置主键。
                //默认开启增量快照。增量快照是一种读取全量数据快照的新机制。与旧的快照读取相比，增量快照有很多优点，包括：
                //读取全量数据时，Source可以是并行读取。
                //读取全量数据时，Source支持chunk粒度的检查点。
                //读取全量数据时，Source不需要获取全局读锁（FLUSH TABLES WITH read lock）。
                //如果您希望Source支持并发读取，每个并发的Reader需要有一个唯一的服务器ID，因此server-id必须是5400-6400这样的范围，并且范围必须大于等于并发数。
                "'scan.incremental.snapshot.chunk.key-column' = 'id' ,\n" +
                //可以指定某一列作为快照阶段切分分片的切分列。无主键表必填，选择的列必须是非空类型（NOT NULL）。
                //有主键的表为选填，仅支持从主键中选择一列。
                "  'table-name' = 'user'\n" +
                ")";
        tEnv.executeSql(sourceTable);
//        tEnv.executeSql("select * from mysql_cdc_source").print();
        String sinkTable = "CREATE TABLE mysql_cdc_sink (" +
                "  id INT,\n" +
                "  username STRING,\n" +
                "  password STRING\n" +
                "  ,PRIMARY KEY (id,username,password) NOT ENFORCED\n" +
                ") WITH (\n" +
                "'connector' = 'jdbc',\n" +
                "'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                "'url' = 'jdbc:mysql://localhost:3306/" + "test_cdc" + "?rewriteBatchedStatements=true',\n" +
                "'username' = 'root',\n" +
                "'password' = 'root',\n" +
                "'table-name' = 'user_new'\n" +
                ")";
        tEnv.executeSql(sinkTable);
        tEnv.executeSql("insert into mysql_cdc_sink select id,username,password from mysql_cdc_source");
    }
}