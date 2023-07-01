package at.cw.watermark

import java.text.SimpleDateFormat
import java.time.Duration


import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.{DataStreamSource, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
 * @Title:
 * @BelongProjecet Flink-Blackberry 
 * @BelongPackage at.cw.watermark
 * @Description:
 * @Copyright time company - Powered By 研发一部
 * @Author: cw
 * @Date: 2023/6/13 13:57
 * @Version V1.0
 *          --------------------输入
 *          s3 1639100010955
 *          s2 1639100009955
 *          s1 1639100008955
 *          s0 1639100007955
 *          s4 1639100011955
 *          s5 1639100012955
 *          s6 1639100013955
 *          s7 1639100016955
 *
 *
 *          --------------------输出
 *
 *          数据:(s3,1639100010955)  ，2021-12-10 09:33:30 ,  当前watermark: 2021-12-10 09:33:28
 *          数据:(s2,1639100009955)  ，2021-12-10 09:33:29 ,  当前watermark: 2021-12-10 09:33:28
 *          数据:(s1,1639100008955)  ，2021-12-10 09:33:28 ,  当前watermark: 2021-12-10 09:33:28
 *          数据:(s0,1639100007955)  ，2021-12-10 09:33:27 ,  当前watermark: 2021-12-10 09:33:28
 *          数据:(s4,1639100011955)  ，2021-12-10 09:33:31 ,  当前watermark: 2021-12-10 09:33:29
 *          数据:(s5,1639100012955)  ，2021-12-10 09:33:32 ,  当前watermark: 2021-12-10 09:33:30
 *          (s2,1639100009955)
 *          (s0,1639100007955)
 *          (s1,1639100008955)
 *          数据:(s6,1639100013955)  ，2021-12-10 09:33:33 ,  当前watermark: 2021-12-10 09:33:31
 *          数据:(s7,1639100016955)  ，2021-12-10 09:33:36 ,  当前watermark: 2021-12-10 09:33:34
 *          (s3,1639100010955)
 *          (s5,1639100012955)
 *          (s4,1639100011955)
 */
object WatermarkLateDemo2 {
  def main(args: Array[String]): Unit = {

  }
}
