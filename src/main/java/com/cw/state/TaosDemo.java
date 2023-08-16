package com.cw.state;

import com.cw.bean.MyGeneratorFunction2;
import com.cw.bean.WaterSensor;
import com.cw.sink.SinkToTDengine;
import com.cw.utils.FilnkUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * TODO 针对每种传感器输出最高的3个水位值
 *
 * @author cjp
 * @version 1.0
 */
public class TaosDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FilnkUtils.getStreamExecutionEnvironmentDev();
        env.setParallelism(1);


        DataGeneratorSource dataGeneratorSource = new DataGeneratorSource(new MyGeneratorFunction2(), Long.MAX_VALUE, Types.POJO(WaterSensor.class));
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.fromSource(dataGeneratorSource, WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((element, ts) -> element.getTs() * 1000L), "data-generator")
                // 指定返回类型
                .returns(new TypeHint<WaterSensor>() {
                });


        sensorDS.print();
        sensorDS.addSink(new SinkToTDengine());

        env.execute();
    }
}
