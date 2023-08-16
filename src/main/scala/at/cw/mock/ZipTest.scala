package at.cw.mock

/**
 * @Title:
 * @BelongProjecet Flink-Blackberry 
 * @BelongPackage at.cw.mock
 * @Description:
 * @Copyright time company - Powered By 研发一部
 * @Author: cw
 * @Date: 2023/8/4 13:33
 * @Version V1.0
 *         ZipTest.scala
 */

object ZipTest { //Scala的拉链操作
  def main(args: Array[String]): Unit = {
    val name = Array("tom", "jerry", "tim")
    val scores = Array(2, 3, 4)
    //将两个数据中的值，组成元组，name为key scores为value
    //数据类型是数组，但是需要注意数组中存储的元素的数据类型是元组
    //谁调用zip谁就是key，后面传入的参数就是value
    val tuples: Array[(String, Int)] = name.zip(scores)
    println(tuples.toBuffer)
    //tuples(0)._2 --> 通过下标获取出数组中第一个元素的值,但是这个元素是元组,继续操作 元组对象,通过_数字方式获取值
    //如果两个集合中数据一致,拉链操作生成的数组的长度是由最小的集合长度
    val name1 = Array("tom", "jerry", "tim")
    val score2 = Array(2, 3)
    //当两个集合中数据不一致时，存储数据最小的那个决定数组长度
    val tuples1: Array[(String, Int)] = name1.zip(score2)
    println(tuples1.toBuffer)
    //拉链扩展、ZipAll诺其中某一集合中的元素少，可以使用默认值填充
    val xs = List(1, 2, 3)
    val zx = List("一", "二", "三", "四", "五")
    val tuples2: List[(Int, Any)] = xs.zipAll(zx, 0, '_')
    println(tuples2)
    //zipwithIndex将集合中元素和索引值进行集合
    val list = List(1, 2, 3, 4)
    val index: List[(Int, Int)] = list.zipWithIndex
    println(index)
    //可以从指定位置开始
    val ts1 = list.zip(Stream from 1)
    println(ts1)


  }

}


