package com.cw.sql;

import com.cw.utils.FilnkUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 */
public class SqlDemo2 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = FilnkUtils.getStreamExecutionEnvironmentDev();

        // TODO 1.创建表环境
        // 1.1 写法一：
//        EnvironmentSettings settings = EnvironmentSettings.newInstance()
//                .inStreamingMode()
//                .build();
//        StreamTableEnvironment tableEnv = TableEnvironment.create(settings);

        // 1.2 写法二
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2.创建表
        tableEnv.executeSql("CREATE TABLE source1 (\n" +
                "dim STRING,\n" +
                "user_id BIGINT,\n" +
                "price BIGINT,\n" +
                "row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n" +
                "WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "'connector' = 'datagen',\n" +
                "'rows-per-second' = '10',\n" +
                "'fields.dim.length' = '1',\n" +
                "'fields.user_id.min' = '1',\n" +
                "'fields.user_id.max' = '100000',\n" +
                "'fields.price.min' = '1',\n" +
                "'fields.price.max' = '100000'\n" +
                ");\n");


        tableEnv.executeSql("CREATE TABLE sink1 (\n" +
                "dim STRING,\n" +
                "pv BIGINT,\n" +
                "sum_price BIGINT,\n" +
                "max_price BIGINT,\n" +
                "min_price BIGINT,\n" +
                "uv BIGINT,\n" +
                "window_start bigint\n" +
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
//        Table source = tableEnv.from("source");
//        Table result = source
//                .where($("id").isGreater(5))
//                .groupBy($("id"))
//                .aggregate($("vc").sum().as("sumVC"))
//                .select($("id"), $("sumVC"));


        // TODO 4.输出表
        // 4.1 sql用法
//        calc pv uv
//        m1(tableEnv);
//        m2(tableEnv);

        tableEnv.executeSql("CREATE TABLE ws (\n" +
                " id INT,\n" +
                " vc INT,\n" +
                " pt AS PROCTIME(), --处理时间\n" +
                " et AS cast(CURRENT_TIMESTAMP as timestamp(3)), --事件时间\n" +
                " WATERMARK FOR et AS et - INTERVAL '5' SECOND --watermark\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second' = '10',\n" +
                " 'fields.id.min' = '1',\n" +
                " 'fields.id.max' = '3',\n" +
                " 'fields.vc.min' = '1',\n" +
                " 'fields.vc.max' = '100'\n" +
                ");");

//        m3(tableEnv);


//        hop
        String sql1 = "select \n" +
                "id,\n" +
                "HOP_START(et, INTERVAL '3' SECOND,INTERVAL '5' SECOND) wstart,\n" +
                "HOP_END(et, INTERVAL '3' SECOND,INTERVAL '5' SECOND) wend,\n" +
                " sum(vc) sumVc\n" +
                "from ws\n" +
                "group by id, HOP(et, INTERVAL '3' SECOND,INTERVAL '5' SECOND);";

//        session
        String sql2="select \n" +
                "id,\n" +
                "SESSION_START(et, INTERVAL '5' SECOND) wstart,\n" +
                "SESSION_END(et, INTERVAL '5' SECOND) wend,\n" +
                "sum(vc) sumVc\n" +
                "from ws\n" +
                "group by id, SESSION(et, INTERVAL '5' SECOND);";

        String sql3="SELECT \n" +
                "window_start, \n" +
                "window_end, \n" +
                "id , SUM(vc) \n" +
                "sumVC\n" +
                "FROM TABLE(\n" +
                " TUMBLE(TABLE ws, DESCRIPTOR(et), INTERVAL '5' SECONDS))\n" +
                "GROUP BY window_start, window_end, id;";
        String sql4="SELECT window_start, window_end, id , SUM(vc) sumVC\n" +
                "FROM TABLE(\n" +
                " HOP(TABLE ws, DESCRIPTOR(et), INTERVAL '5' SECONDS , INTERVAL \n" +
                "'10' SECONDS))\n" +
                "GROUP BY window_start, window_end, id;";
        String sql5="SELECT \n" +
                "window_start, \n" +
                "window_end, \n" +
                "id , \n" +
                "SUM(vc) sumVC\n" +
                "FROM TABLE(\n" +
                " CUMULATE(TABLE ws, DESCRIPTOR(et), INTERVAL '2' SECONDS , \n" +
                "INTERVAL '6' SECONDS))\n" +
                "GROUP BY window_start, window_end, id;";
        String sql6="SELECT \n" +
                "window_start, \n" +
                "window_end, \n" +
                "id , \n" +
                "SUM(vc) sumVC\n" +
                "FROM TABLE(\n" +
                " TUMBLE(TABLE ws, DESCRIPTOR(et), INTERVAL '5' SECONDS))\n" +
                "GROUP BY window_start, window_end,\n" +
                "rollup( (id) )\n" +
                "-- cube( (id) )\n" +
                "-- grouping sets( (id),() )\n" +
                ";";
//        统计每个传感器前 10 秒到现在收到的水位数据条数
        String sql7="SELECT \n" +
                " id, \n" +
                " et, \n" +
                " vc,\n" +
                " count(vc) OVER (\n" +
                " PARTITION BY id\n" +
                " ORDER BY et\n" +
                " RANGE BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW\n" +
                " ) AS cnt\n" +
                "FROM ws";
        String sql8="SELECT \n" +
                " id, \n" +
                " et, \n" +
                " vc,\n" +
                "count(vc) OVER w AS cnt,\n" +
                "sum(vc) OVER w AS sumVC\n" +
                "FROM ws\n" +
                "WINDOW w AS (\n" +
                " PARTITION BY id\n" +
                " ORDER BY et\n" +
                " RANGE BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW\n" +
                ")\n";

//        统计每个传感器前 5 条到现在数据的平均水位
        String sql9="SELECT \n" +
                " id, \n" +
                " et, \n" +
                " vc,\n" +
                " avg(vc) OVER (\n" +
                " PARTITION BY id\n" +
                " ORDER BY et\n" +
                " ROWS BETWEEN 5 PRECEDING AND CURRENT ROW\n" +
                ") AS avgVC\n" +
                "FROM ws";
        String sql10="SELECT \n" +
                " id, \n" +
                " et, \n" +
                " vc,\n" +
                "avg(vc) OVER w AS avgVC,\n" +
                "count(vc) OVER w AS cnt\n" +
                "FROM ws\n" +
                "WINDOW w AS (\n" +
                " PARTITION BY id\n" +
                " ORDER BY et\n" +
                " ROWS BETWEEN 5 PRECEDING AND CURRENT ROW\n" +
                ")";

//        取每个传感器最高的 3 个水位值
        String sql11="select \n" +
                " id,\n" +
                " et,\n" +
                " vc,\n" +
                " rownum\n" +
                "from \n" +
                "(\n" +
                " select \n" +
                " id,\n" +
                " et,\n" +
                " vc,\n" +
                " row_number() over(\n" +
                " partition by id \n" +
                " order by vc desc \n" +
                " ) as rownum\n" +
                " from ws\n" +
                ")\n" +
                "where rownum<=3;";

//        对每个传感器的水位值去重
        String sql12="select \n" +
                " id,\n" +
                " et,\n" +
                " vc,\n" +
                " rownum\n" +
                "from \n" +
                "(\n" +
                " select \n" +
                " id,\n" +
                " et,\n" +
                " vc,\n" +
                " row_number() over(\n" +
                " partition by id,vc\n" +
                " order by et\n" +
                " ) as rownum\n" +
                " from ws\n" +
                ")\n" +
                "where rownum=1;\n";
        String sql13="CREATE TABLE ws1 (\n" +
                " id INT,\n" +
                " vc INT,\n" +
                " pt AS PROCTIME(), --处理时间\n" +
                " et AS cast(CURRENT_TIMESTAMP as timestamp(3)), --事件时间\n" +
                " WATERMARK FOR et AS et - INTERVAL '0.001' SECOND --watermark\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second' = '1',\n" +
                " 'fields.id.min' = '3',\n" +
                " 'fields.id.max' = '5',\n" +
                " 'fields.vc.min' = '1',\n" +
                " 'fields.vc.max' = '100'\n" +
                ");\n";

        String sql15="CREATE TABLE ws1 (\n" +
                " id INT,\n" +
                " vc INT,\n" +
                " pt AS PROCTIME(), --处理时间\n" +
                " et AS cast(CURRENT_TIMESTAMP as timestamp(3)), --事件时间\n" +
                " WATERMARK FOR et AS et - INTERVAL '0.001' SECOND --watermark\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second' = '1',\n" +
                " 'fields.id.min' = '3',\n" +
                " 'fields.id.max' = '5',\n" +
                " 'fields.vc.min' = '1',\n" +
                " 'fields.vc.max' = '100'\n" +
                ");";
        tableEnv.executeSql(sql15);

        String sql14="SELECT *\n" +
                "FROM ws\n" +
                "INNER JOIN ws1\n" +
                "ON ws.id = ws1.id";

        /**
         * SELECT *
         * FROM ws
         * LEFT JOIN ws1
         * ON ws.id = ws1.id;
         *
         * SELECT *
         * FROM ws
         * RIGHT JOIN ws1
         * ON ws.id = ws1.id;
         *
         * SELECT *
         * FROM ws
         * FULL OUTER JOIN ws1
         * ON ws.id = ws.id;
         */
        String sql16="SELECT *\n" +
                "FROM ws,ws1\n" +
                "WHERE ws.id = ws1. id\n" +
                "AND ws.et BETWEEN ws1.et - INTERVAL '2' SECOND AND ws1.et + INTERVAL \n" +
                "'2' SECOND";
        String sql17="";
        String sql18="";
        String sql19="";
        String sql20="";
        String sql21="";

        tableEnv.executeSql(sql16).print();

        tableEnv.executeSql("select 1,2");

        tableEnv.executeSql("select 1,2");

        // 4.2 tableapi用法
//        result.executeInsert("sink");
    }

    private static void m3(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("select \n" +
                "id,\n" +
                "TUMBLE_START(et, INTERVAL '5' SECOND) wstart,\n" +
                "TUMBLE_END(et, INTERVAL '5' SECOND) wend,\n" +
                "sum(vc) sumVc\n" +
                "from ws\n" +
                "group by id, TUMBLE(et, INTERVAL '5' SECOND);\n").print();
    }

    private static void m2(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("SELECT\n" +
                " supplier_id\n" +
                ", rating\n" +
                ", product_id\n" +
                ", COUNT(*) as cnt\n" +
                "FROM (\n" +
                "VALUES\n" +
                " ('supplier1', 'product1', 4),\n" +
                " ('supplier1', 'product2', 3),\n" +
                " ('supplier2', 'product3', 3),\n" +
                " ('supplier2', 'product4', 4)\n" +
                ")\n" +
                "-- 供应商 id、产品 id、评级\n" +
                "AS Products(supplier_id, product_id, rating) \n" +
                "GROUP BY GROUPING SETS(\n" +
                " (supplier_id, product_id, rating),\n" +
                " (supplier_id, product_id),\n" +
                " (supplier_id, rating),\n" +
                " (supplier_id),\n" +
                " (product_id, rating),\n" +
                " (product_id),\n" +
                " (rating),\n" +
                " ()\n" +
                ");").print();
    }

    private static void m1(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql(" insert into sink1\n" +
                "select dim,\n" +
                "count(*) as pv,\n" +
                "sum(price) as sum_price,\n" +
                "max(price) as max_price,\n" +
                "min(price) as min_price,\n" +
                "count(distinct user_id) as uv,\n" +
                "cast((UNIX_TIMESTAMP(CAST(row_time AS STRING))) / 60 as bigint) as window_start\n" +
                "from source1\n" +
                "group by\n" +
                "dim,\n" +
                "cast((UNIX_TIMESTAMP(CAST(row_time AS STRING))) / 60 as bigint)\n").print();
    }
}
