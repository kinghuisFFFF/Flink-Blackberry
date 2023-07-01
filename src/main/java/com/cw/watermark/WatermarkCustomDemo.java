package com.cw.watermark;

import com.cw.bean.MyGeneratorFunction2;
import com.cw.bean.WaterSensor;
import com.cw.utils.FilnkUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 */
public class WatermarkCustomDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FilnkUtils.getStreamExecutionEnvironmentDev();
        env.setParallelism(1);


        // 默认周期 200ms
        env.getConfig().setAutoWatermarkInterval(2000);


        DataGeneratorSource dataGeneratorSource = new DataGeneratorSource(new MyGeneratorFunction2(), Long.MAX_VALUE, Types.POJO(WaterSensor.class));
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator")
                // 指定返回类型
                .returns(new TypeHint<WaterSensor>() {
                });

        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
                // TODO 指定 自定义的 生成器
                // 1.自定义的 周期性生成
//                .<WaterSensor>forGenerator(ctx -> new MyPeriodWatermarkGenerator<>(3000L))
                // 2.自定义的 断点式生成
                .<WaterSensor>forGenerator(ctx -> new MyPuntuatedWatermarkGenerator<>(3000L))
                .withTimestampAssigner(
                        (element, recordTimestamp) -> {
                            return element.getTs() * 1000L;
                        });

        SingleOutputStreamOperator<WaterSensor> sensorDSwithWatermark = sensorDS.assignTimestampsAndWatermarks(watermarkStrategy);


        sensorDSwithWatermark.keyBy(sensor -> sensor.getId())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(
                        new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {

                            @Override
                            public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                                long startTs = context.window().getStart();
                                long endTs = context.window().getEnd();
                                String windowStart = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss.SSS");
                                String windowEnd = DateFormatUtils.format(endTs, "yyyy-MM-dd HH:mm:ss.SSS");

                                long count = elements.spliterator().estimateSize();

                                out.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + ")包含" + count + "条数据===>" + elements.toString());
                            }
                        }
                )
                .print();

        env.execute();
    }
}
