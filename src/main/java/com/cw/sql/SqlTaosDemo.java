package com.cw.sql;

import cn.hutool.core.util.XmlUtil;
import com.cw.utils.FilnkUtils;
import com.cw.utils.ReadXmlUtils;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.w3c.dom.Document;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.table.api.Expressions.$;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 */
public class SqlTaosDemo {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        StreamExecutionEnvironment env = FilnkUtils.getStreamExecutionEnvironmentDev();
        env.setParallelism(2);
        // checkpoint设置
        env.enableCheckpointing(5000L,CheckpointingMode.EXACTLY_ONCE);
//        hdfs:///hadoop31:9000/flink/checkpoints
//        hdfs:///hadoop31:9000/flink/checkpoints
        FileSystemCheckpointStorage checkpointStorage = new FileSystemCheckpointStorage("hdfs://hadoop31:9000/flink/torrent/statebackend/job1");
        env.getCheckpointConfig().setCheckpointStorage(checkpointStorage);

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
        tableEnv.getConfig().getConfiguration().setString("pipeline.name","LABEL SqlTaosDemo");

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




        Document document = XmlUtil.readXML("sql-map/ods/SQL_Taos.xml");
        String sql7 = ReadXmlUtils.getTagValueByPath(document, "//sqls/create/sql01");
        String sql8 = ReadXmlUtils.getTagValueByPath(document, "//sqls/create/sql02");
        String sql9 = ReadXmlUtils.getTagValueByPath(document, "//sqls/create/sql05");
        String sql10 = ReadXmlUtils.getTagValueByPath(document, "//sqls/create/sql07");
        String sql11 = ReadXmlUtils.getTagValueByPath(document, "//sqls/create/sql08");

//        sink mysql
        String action_sql11 = ReadXmlUtils.getTagValueByPath(document, "//sqls/action/sql01");
//        sink mongodb
        String action_sql12 = ReadXmlUtils.getTagValueByPath(document, "//sqls/action/sql02");
        String action_sql13 = ReadXmlUtils.getTagValueByPath(document, "//sqls/action/sql03");
        // TODO 2.创建表
        System.out.println(sql7);
        tableEnv.executeSql(sql7);
        tableEnv.executeSql(sql8);
        tableEnv.executeSql(sql9);
        tableEnv.executeSql(sql10);
        tableEnv.executeSql(sql11);

        //// 写入hive的t2表
//        statementSet.addInsertSql(action_sql9);
//        statementSet.addInsertSql(action_sql11);
        statementSet.addInsertSql(action_sql12);
        statementSet.addInsertSql(action_sql13);
        TableResult tableResult = statementSet.execute();


        CompletableFuture<JobStatus> jobStatus = tableResult.getJobClient().get().getJobStatus();
        System.out.println("status: "+jobStatus);
        System.out.println("jobStatus status: "+jobStatus.get());

        tableEnv.executeSql("show databases").print();
//        tableEnv.executeSql("show tables").print();

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
