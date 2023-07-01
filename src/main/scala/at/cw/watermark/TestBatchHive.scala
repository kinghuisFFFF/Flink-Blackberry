package at.cw.watermark

/**
 * @Title:
 * @BelongProjecet Flink-Blackberry 
 * @BelongPackage at.cw.watermark
 * @Description:
 * @Copyright time company - Powered By 研发一部
 * @Author: cw
 * @Date: 2023/6/29 10:45
 * @Version V1.0
 */

import org.apache.flink.table.api._
import org.apache.flink.table.catalog.hive.HiveCatalog

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
 *  flink 批量读写hive
 */
object TestBatchHive{
 def main(args: Array[String]): Unit = {
   System.setProperty("HADOOP_USER_NAME","root")

    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
//      .inBatchMode()
      .inStreamingMode()
      .build()

    val tableEnv: TableEnvironment = TableEnvironment.create(settings)

    // 测试flink sql  好不好用
    val res = tableEnv.sqlQuery("select '1', '1', 1, '1', '1', '1'")
    val r = res.execute()
    r.print()

   val name            = "myhive"
   val defaultDatabase = "ods"
   val hiveConfDir     = "D:\\ws\\GitHub\\Flink-Blackberry\\src\\main\\resources"
//    val name = "myhive" // Catalog名称，定义一个唯一的名称表示
//    val defaultDatabase = "default" // 默认数据库名称
//    val hiveConfDir = "/etc/hive/conf" // hive-site.xml路径
    val version = "3.1.2" // Hive版本号

    val hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version)

    val statementSet: StatementSet = tableEnv.createStatementSet()
    tableEnv.registerCatalog(name, hive)

    tableEnv.useCatalog(name)

//   tableEnv.executeSql("CREATE TABLE Orders3 (\n    order_number BIGINT,\n    price        DECIMAL(32,2),\n    buyer        ROW<first_name STRING, last_name STRING>,\n    order_time   TIMESTAMP(3)\n) WITH (\n  'connector' = 'datagen'\n)")
   tableEnv.executeSql("desc Orders3").print()
//   tableEnv.executeSql("select * from Orders3").print()

    tableEnv.getConfig.setSqlDialect(SqlDialect.HIVE)
    tableEnv.useDatabase("ods")

    // 设置读取hive的并行度
    val configuration = tableEnv.getConfig.getConfiguration

    configuration.setString("table.exec.hive.infer-source-parallelism", "false")
    configuration.setString("table.exec.hive.infer-source-parallelism.max", "15")
    configuration.setString("table.exec.hive.fallback-mapred-reader", "true")


    val sqlResult: Table = tableEnv.sqlQuery(
      """
        | select * from Orders3
        |""".stripMargin)

    //    这种方式也可以
    //    statementSet.addInsert("fs_table_dst", sqlResult)
    //    statementSet.execute()

    // register the view named 'fs_table_source.View' in the catalog named 'myhive'
    // in the database named 'default'
    tableEnv.createTemporaryView("fs_table_source.View", sqlResult)

    // 统计总的数量
    val total = tableEnv.executeSql("select count(*) from fs_table_source.View")
    total.print()

    // 写入hive的fs_table_dst表
    tableEnv.executeSql("insert into orders select * from fs_table_source.View")

  }
}

