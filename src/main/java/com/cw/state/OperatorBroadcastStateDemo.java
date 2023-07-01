package com.cw.state;

import com.cw.bean.MyGeneratorFunction2;
import com.cw.bean.WaterSensor;
import com.cw.utils.FilnkUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * TODO 水位超过指定的阈值发送告警，阈值可以动态修改
 *
 * @author cjp
 * @version 1.0
 */
public class OperatorBroadcastStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FilnkUtils.getStreamExecutionEnvironmentDev();
        env.setParallelism(2);


        // 数据流
//        SingleOutputStreamOperator<WaterSensor> sensorDS = env
//                .socketTextStream("tencentcloud.yawujia.cn", 8082)
//                .map(new WaterSensorMapFunction());

        DataGeneratorSource dataGeneratorSource = new DataGeneratorSource(new MyGeneratorFunction2(), Long.MAX_VALUE, Types.POJO(WaterSensor.class));
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.fromSource(dataGeneratorSource, WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((element, ts) -> element.getTs() * 1000L), "data-generator")
                // 指定返回类型
                .returns(new TypeHint<WaterSensor>() {
                });

        // 配置流（用来广播配置）
        DataStreamSource<String> configDS = env.socketTextStream("tencentcloud.yawujia.cn", 8888);

        // TODO 1. 将 配置流 广播
        MapStateDescriptor<String, Integer> broadcastMapState = new MapStateDescriptor<>("broadcast-state", Types.STRING, Types.INT);
        BroadcastStream<String> configBS = configDS.broadcast(broadcastMapState);

        // TODO 2.把 数据流 和 广播后的配置流 connect
        BroadcastConnectedStream<WaterSensor, String> sensorBCS = sensorDS.connect(configBS);

        // TODO 3.调用 process
        sensorBCS
                .process(
                        new BroadcastProcessFunction<WaterSensor, String, String>() {
                            /**
                             * 数据流的处理方法： 数据流 只能 读取 广播状态，不能修改
                             * @param value
                             * @param ctx
                             * @param out
                             * @throws Exception
                             */
                            @Override
                            public void processElement(WaterSensor value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                                // TODO 5.通过上下文获取广播状态，取出里面的值（只读，不能修改）
                                ReadOnlyBroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(broadcastMapState);
                                Integer threshold = broadcastState.get("threshold");
                                // 判断广播状态里是否有数据，因为刚启动时，可能是数据流的第一条数据先来
                                threshold = (threshold == null ? 0 : threshold);
                                if (value.getVc() > threshold) {
                                    out.collect(value + ",水位超过指定的阈值：" + threshold + "!!!");
                                }

                            }

                            /**
                             * 广播后的配置流的处理方法:  只有广播流才能修改 广播状态
                             * @param value
                             * @param ctx
                             * @param out
                             * @throws Exception
                             */
                            @Override
                            public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                                // TODO 4. 通过上下文获取广播状态，往里面写数据
                                BroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(broadcastMapState);
                                broadcastState.put("threshold", Integer.valueOf(value));

                            }
                        }

                )
                .print();

        env.execute();
    }
}

