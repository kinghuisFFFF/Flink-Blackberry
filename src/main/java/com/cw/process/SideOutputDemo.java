package com.cw.process;

import com.cw.bean.MyGeneratorFunction2;
import com.cw.bean.WaterSensor;
import com.cw.utils.FilnkUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 */
public class SideOutputDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FilnkUtils.getStreamExecutionEnvironmentDev();
        env.setParallelism(1);


//        SingleOutputStreamOperator<WaterSensor> sensorDS = env
//                .socketTextStream("tencentcloud.yawujia.cn", 8082)
//                .map(new WaterSensorMapFunction())
//                .assignTimestampsAndWatermarks(
//                        WatermarkStrategy
//                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
//                                .withTimestampAssigner((element, ts) -> element.getTs() * 1000L)
//                );

        DataGeneratorSource dataGeneratorSource = new DataGeneratorSource(new MyGeneratorFunction2(), Long.MAX_VALUE, Types.POJO(WaterSensor.class));
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.fromSource(dataGeneratorSource, WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((element, ts) -> element.getTs() * 1000L), "data-generator")
                // 指定返回类型
                .returns(new TypeHint<WaterSensor>() {
                });


        OutputTag<String> warnTag = new OutputTag<>("warn", Types.STRING);
        SingleOutputStreamOperator<WaterSensor> process = sensorDS.keyBy(sensor -> sensor.getId())
                .process(
                        new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                            @Override
                            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                                // 使用侧输出流告警
                                if (value.getVc() > 10) {
                                    ctx.output(warnTag, "当前水位=" + value.getVc() + ",大于阈值10！！！");
                                }
                                // 主流正常 发送数据
                                out.collect(value);
                            }
                        }
                );

        process.print("主流");
        process.getSideOutput(warnTag).printToErr("warn");


        env.execute();
    }
}
