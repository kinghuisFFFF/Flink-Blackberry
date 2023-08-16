package com.cw.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cw.app.func.AsyncDimFunction;
import com.cw.bean.GxPower02;
import com.cw.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.concurrent.TimeUnit;
//数据流: Web/App -> Nginx -> SpringBoot -> Mysql -> FlinkApp -> Kafka(ods) -> FlinkApp -> Kafka(dwd/dim) ->
//       FlinkApp -> Kafka(dwm)
//进程:           MockDB                 ->Mysql -> FlinkCDCApp -> Kafka(ZK) -> BaseDBApp -> Kafka(Phoenix)          -> GxPower02App -> Kafka
public class PowerWideApp {

    public static void main(String[] args) throws Exception {

        //TODO 1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        env.enableCheckpointing(5000L); // 5s
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L); // 1min,时间太短容易报错。之前设置10s经常报错 10000L
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));
//        env.setStateBackend(new FsStateBackend("hdfs://edh/flink/job3/ck/"));
//        System.setProperty("HADOOP_USER_NAME", "hdfs");

        //TODO 2 读取Kafka dwd_order_info dwd_order_detail主题的数据
        String powerInfoSourceTopic = "dwd_gx_power_02";
        String powerWideSinkTopic = "dwm_gx_power_02";
        String groupId = "power_app_group";
        FlinkKafkaConsumer<String> GxPower02Source = MyKafkaUtil.getKafkaSource(groupId, powerInfoSourceTopic);

        DataStreamSource<String> powerInfoJsonStrDS = env.addSource(GxPower02Source);

//        powerInfoJsonStrDS.print("原始PowerInfo>>>>>>>>>>");

        //TODO 3 将2个流转换为JavaBean并提取数据中的时间戳生成WaterMark
        SingleOutputStreamOperator<GxPower02> gxPower02DS = powerInfoJsonStrDS.map(data -> {

            GxPower02 GxPower02 = JSON.parseObject(data, GxPower02.class);

            //补充其中的时间字段
            String create_time = GxPower02.getHtime(); //yyyy-MM-dd HH:mm:ss
            System.out.println(create_time);
            long currentTimeMillis = System.currentTimeMillis();
            GxPower02.setWm_htime(currentTimeMillis);
            return GxPower02;
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<GxPower02>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<GxPower02>() {
                    @Override
                    public long extractTimestamp(GxPower02 element, long recordTimestamp) {
                        return element.getWm_htime();
                    }
                }));

        //TODO 5 查询Phoenix,补全维度信息
        //5.1 关联工厂维度
        //异步查询：Flink处理数据时候，遇到比较耗时的操作时，需要异步处理数据。
        SingleOutputStreamOperator<GxPower02> powerWideWithNameDS = AsyncDataStream.unorderedWait(gxPower02DS,
                new AsyncDimFunction<GxPower02>("DIM_PRODUCER") {

                    @Override
                    public String getKey(GxPower02 GxPower02) {
                        return GxPower02.getProducerId();
                    }

                    @Override
                    public void join(GxPower02 GxPower02, JSONObject dimJSON) {

                        if (dimJSON != null) {
                            String pname = dimJSON.getString("NAME");
                            GxPower02.setProducerName(pname);
                        }
                    }
                },
                100,
                TimeUnit.SECONDS);
        powerWideWithNameDS.print("Name >>>>>>>>>>>>");


        //TODO 6 将数据写入Kafka
        //这里一定要toJsonString
        powerWideWithNameDS.map(JSON::toJSONString)
                .addSink(MyKafkaUtil.getKafkaSink(powerWideSinkTopic));

        //TODO 7 启动任务
        env.execute();

    }

}
