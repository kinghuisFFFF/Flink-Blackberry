package com.cw.sql;

import com.cw.utils.FilnkUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 */
public class SqlDemo3 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = FilnkUtils.getStreamExecutionEnvironmentDev();
        env.setParallelism(2);
        env.enableCheckpointing(5000L);

        // TODO 1.创建表环境
        // 1.1 写法一：
//        EnvironmentSettings settings = EnvironmentSettings.newInstance()
//                .inStreamingMode()
//                .build();
//        StreamTableEnvironment tableEnv = TableEnvironment.create(settings);

        // 1.2 写法二
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(10));
        tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);

        String name = "myhive";
        String defaultDatabase = "ods";
        String hiveConfDir = "D:\\ws\\GitHub\\Flink-Blackberry\\src\\main\\resources";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        StreamStatementSet statementSet = tableEnv.createStatementSet();
        tableEnv.registerCatalog("myhive", hive);

// set the HiveCatalog as the current catalog of the session
//        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
//        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tableEnv.useCatalog(name);
        tableEnv.useDatabase(defaultDatabase);

//        设置读取hive的并行度
        Configuration cfg = tableEnv.getConfig().getConfiguration();
        cfg.setString("table.exec.hive.infer-source-parallelism", "false");
        cfg.setString("table.exec.hive.infer-source-parallelism.max", "15");
        cfg.setString("table.exec.hive.fallback-mapred-reader", "true");

        // TODO 2.创建表
        tableEnv.executeSql("CREATE temporary TABLE source ( \n" +
                "    id INT, \n" +
                "    ts BIGINT, \n" +
                "    vc INT\n" +
                ") WITH ( \n" +
                "    'connector' = 'datagen', \n" +
                "    'rows-per-second'='1', \n" +
                "    'fields.id.kind'='random', \n" +
                "    'fields.id.min'='1', \n" +
                "    'fields.id.max'='10', \n" +
                "    'fields.ts.kind'='sequence', \n" +
                "    'fields.ts.start'='1', \n" +
                "    'fields.ts.end'='1000000', \n" +
                "    'fields.vc.kind'='random', \n" +
                "    'fields.vc.min'='1', \n" +
                "    'fields.vc.max'='100'\n" +
                ");\n");

        //// 写入hive的t2表
        statementSet.addInsertSql("insert into t2 select * from source");
        statementSet.addInsertSql("insert into t3 select * from source");
        TableResult tableResult = statementSet.execute();
        System.out.println(tableResult.getJobClient().get().getJobStatus());

//        tableEnv.executeSql("insert into t2 select * from source");
        tableEnv.executeSql("show databases").print();
        tableEnv.executeSql("show tables").print();
//        tableEnv.executeSql("select * from t1 limit 10");
//        tableEnv.executeSql("select * from order_info limit 10");

        tableEnv.executeSql("CREATE temporary  TABLE sink (\n" +
                "    id INT, \n" +
                "    sumVC INT \n" +
                ") WITH (\n" +
                "'connector' = 'print'\n" +
                ");\n");

        // TODO 3.执行查询
        // 3.1 使用sql进行查询
//        Table table = tableEnv.sqlQuery("select id,sum(vc) as sumVC from source where id>5 group by id ;");
        // 把table对象，注册成表名
//        tableEnv.createTemporaryView("tmp", table);
//        tableEnv.sqlQuery("select * from tmp where id > 7");

        // 3.2 用table api来查询
        Table source = tableEnv.from("source");
        Table result = source
                .where($("id").isGreater(5))
                .groupBy($("id"))
                .aggregate($("vc").sum().as("sumVC"))
                .select($("id"), $("sumVC"));


        // TODO 4.输出表
        // 4.1 sql用法
//        tableEnv.executeSql("insert into sink select * from tmp");
        // 4.2 tableapi用法
//        result.executeInsert("sink");
    }
}
