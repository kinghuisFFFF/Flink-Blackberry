package at.cw.mock

/**
 * @Title:
 * @BelongProjecet Flink-Blackberry 
 * @BelongPackage at.cw.mock
 * @Description:
 * @Copyright time company - Powered By 研发一部
 * @Author: cw
 * @Date: 2023/8/4 13:36
 * @Version V1.0
 */

import java.sql.{Connection, DriverManager}

object JBJDBC extends App {
  // 访问本地MySQL服务器，通过3306端口访问mysql数据库
  val url = "jdbc:mysql://localhost:3306/datassets_4300?useUnicode=true&characterEncoding=utf-8&useSSL=false"
  //驱动名称
  val driver = "com.mysql.jdbc.Driver"
  //用户名
  val username = "root"
  //密码
  val password = "123456"
  //初始化数据连接
  var connection: Connection = _
  try {
    //注册Driver
    Class.forName(driver)
    //得到连接
    connection = DriverManager.getConnection(url, username, password)
    val statement = connection.createStatement
  }

  val statement = connection.createStatement
  //执行查询语句，并返回结果
  val rs = statement.executeQuery("SELECT id,name FROM persons")
  //打印返回结果
  while (rs.next) {
    val id = rs.getString("id")
    val name = rs.getString("name")
    //      println(name+"\t"+num)
    println("id = %s ,name = %s".format(id, name))
  }
  println("查询数据完成！")

//  val statement = connection.createStatement
  //执行查询语句，并返回结果
  val rs2 = statement.executeUpdate("SQL语句")



}

