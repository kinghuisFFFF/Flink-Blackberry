package com.cw.test;
 
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
 
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
 
public class FlinkCdc20MysqlToMysql {
    public static void main(String[] args) throws Exception {
 
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String sourceSql = "CREATE TABLE IF NOT EXISTS mySqlSource (" +
                "id BIGINT primary key, " +
                "name string ," +
                "age int," +
                "dt string" +
                ") with ( " +
                " 'connector' = 'mysql-cdc', " +
                " 'scan.startup.mode' = 'latest-offset', " +
                " 'hostname' = '192.168.1.180', " +
                " 'port' = '3306', " +
                " 'username' = 'root', " +
                " 'password' = '123456', " +
                " 'database-name' = 'test', " +
                " 'table-name' = 'Flink_iceberg' " +
                ")";
 
        String sinkSql = " CREATE TABLE IF NOT EXISTS mySqlSink (" +
                "id BIGINT primary key , " +
                "name string ," +
                "age int," +
                "dt string" +
                ") with (" +
                " 'connector' = 'jdbc'," +
                " 'url' = 'jdbc:mysql://192.168.1.180:3306/test'," +
                "'table-name' = 'Flink_iceberg-cdc'," +
                " 'username' = 'root'," +
                " 'password' = '123456' " +
                " )";
        tableEnv.executeSql(sourceSql);
        tableEnv.executeSql(sinkSql);
        tableEnv.executeSql("insert  into  mySqlSink select * from mySqlSource ");
//        env.execute("FlinkCdc20MysqlToMysql");
    }
}