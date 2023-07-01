package at.cw.watermark

import cn.hutool.core.io.resource.ResourceUtil
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.table.api.EnvironmentSettings._
import org.apache.flink.table.api.Expressions.currentTimestamp
import org.apache.flink.table.api.{SqlDialect, TableEnvironment}
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.flink.types.Row

import scala.collection.JavaConverters.seqAsJavaListConverter


//执行命令
//--------------------- 任务类的完整包路径 -- 程序包 -- 参数名称 ---- 参数 ------------------
//flink run -c y.l.test.HIveConnectTest test.jar --hiveCfgDir hive-site.xml文件所在的目录
object HIveConnectTest {

  //测试数据结构
  case class Person(name: String, age: Int, gender: String)

  def main(args: Array[String]): Unit = {
    //参数配置(flink参数获取工具类)
    val parameterTool = ParameterTool.fromArgs(args)
    //hive-site.xml 配置文件所在的目录 --hiveCfg xxx(目录全路径)
//    val hiveConfigDir = parameterTool.get("hiveCfgDir", ResourceUtil.getResource("hive/conf").getPath)
    val hiveConfigDir = parameterTool.get("hiveCfgDir", ResourceUtil.getResource("").getPath)
    //设置为批处理
    val environmentSettings = newInstance().inBatchMode().build()
    //使用TableApi
    val tableEnv = TableEnvironment.create(environmentSettings)
    //配置hive元数据
    //配置hive Catalog (元数据)
    val catalogName = "myhive"
    val hiveCatalog = new HiveCatalog(catalogName, "ods", hiveConfigDir)
    tableEnv.registerCatalog(catalogName, hiveCatalog)
    // 使用注册的 hive catalog
    tableEnv.useCatalog(catalogName)
    tableEnv.executeSql("show catalogs").print()
    //切换方言
//    tableEnv.getConfig.setSqlDialect(SqlDialect.HIVE)
    //创建测试表
    val person = "person"
//    stored as parquet
    tableEnv.executeSql(
      s"""
         |create table if not exists $person (
         | name string,
         | age int,
         | gender string,
         | sink_date_time timestamp(9)
         |)
         |""".stripMargin)
    //切换方言
//    tableEnv.getConfig.setSqlDialect(SqlDialect.DEFAULT)
//    tableEnv.getConfig.setSqlDialect(SqlDialect.HIVE)
    import org.apache.flink.table.api.Expressions.$
    //创建测试数据 并将数据插入到 hive的测试表中
    tableEnv.fromValues(
      List(
        Person("测试1", 15, "男"),
        Person("测试2", 16, "女"),
        Person("测试3", 17, "男"),
        Person("测试4", 16, "女"),
        Person("测试5", 15, "男")
      ).map(p => Row.of(p.name, Int.box(p.age), p.gender)).asJava)
      .as("name", "age", "gender")
      .addColumns(currentTimestamp() as "sink_date_time")
//      .select($("name"), $("age"), $"gender", $"sink_date_time")
      .select($("name"), $("age"), $("gender"), $("sink_date_time"))
//    Expressions.$("x")
      .executeInsert(person)
  }

}
