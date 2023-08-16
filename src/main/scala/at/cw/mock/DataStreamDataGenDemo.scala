package at.cw.mock

/**
 * @Title:
 * @BelongProjecet Flink-Blackberry 
 * @BelongPackage at.cw.mock
 * @Description:
 * @Copyright time company - Powered By 研发一部
 * @Author: cw
 * @Date: 2023/7/4 17:17
 * @Version V1.0
 */

import org.apache.commons.math3.random.RandomDataGenerator
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.functions.source.datagen.{DataGeneratorSource, RandomGenerator, SequenceGenerator}
import org.apache.flink.streaming.api.scala._

object DataStreamDataGenDemo {
  def main(args: Array[String]): Unit = {

    val configuration = new Configuration()

    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration)

    env.setParallelism(1)
//    全局禁用算子链
    env.disableOperatorChaining()


    // 1.0 生成随机数据RandomGenerator
    val orderInfods = env.addSource(
      new DataGeneratorSource[OrderInfo](
        new RandomGenerator[OrderInfo]() {
          override def next() = {
            OrderInfo(
              random.nextInt(1, 100000),
              random.nextLong(1, 100000),
              random.nextUniform(1, 1000),
              System.currentTimeMillis()
            )
          }
        }
      ))
    // 1.0 生成序列数据SequenceGenerator
    val userInfods = env.addSource(
      new DataGeneratorSource[UserInfo](
        new SequenceGenerator[UserInfo](1, 1000000) {
          val random = new RandomDataGenerator()

          override def next() = {
            UserInfo(
              // poll拿出不放回
              // peek拿出放回
              valuesToEmit.poll().intValue(),
              valuesToEmit.poll().longValue(),
              random.nextInt(1, 100),
              random.nextInt(0, 1)
            )
          }
        }
      ))
    orderInfods.print("orderinfo>>>>>>>>>>>>>>>>")
    userInfods.print("userinfo>>>")
    env.execute()

  }
}

case class OrderInfo(
                      id: Int,
                      user_id: Long,
                      total_amount: Double,
                      create_time: Long
                    )

case class UserInfo(
                     id: Int,
                     user_id: Long,
                     age: Int,
                     sex: Int
                   )

