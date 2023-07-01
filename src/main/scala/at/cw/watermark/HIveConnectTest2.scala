package at.cw.watermark

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.catalog.hive.HiveCatalog

/**
 * @Title:
 * @BelongProjecet Flink-Blackberry 
 * @BelongPackage at.cw.watermark
 * @Description:
 * @Copyright time company - Powered By 研发一部
 * @Author: cw
 * @Date: 2023/6/26 18:23
 * @Version V1.0
 */
object HIveConnectTest2 {
  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings.inStreamingMode()
    val tableEnv = TableEnvironment.create(settings)

    val name            = "myhive"
    val defaultDatabase = "ods"
    val hiveConfDir     = "D:\\ws\\GitHub\\Flink-Blackberry\\src\\main\\resources"

    val hive = new HiveCatalog(name, defaultDatabase, hiveConfDir)
    tableEnv.registerCatalog("myhive", hive)

    // set the HiveCatalog as the current catalog of the session
    tableEnv.useCatalog("myhive")

    tableEnv.executeSql("show databases")
    tableEnv.executeSql("show databases")
  }

}
