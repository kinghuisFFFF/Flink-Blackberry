package com.cw.transfrom;

import com.cw.bean.WaterSensor;
import com.cw.utils.FilnkUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 */
public class FilterDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FilnkUtils.getStreamExecutionEnvironmentDev();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        // TODO filter： true保留，false过滤掉
//        SingleOutputStreamOperator<WaterSensor> filter = sensorDS.filter(new FilterFunction<WaterSensor>() {
//            @Override
//            public boolean filter(WaterSensor value) throws Exception {
//                return "s1".equals(value.getId());
//            }
//        });

//        SingleOutputStreamOperator<WaterSensor> filter = sensorDS.filter(new FilterFunctionImpl("s1"));
        SingleOutputStreamOperator<WaterSensor> filter = sensorDS.filter(sensor->"s1".equals(sensor.id));
//        SingleOutputStreamOperator<WaterSensor> filter = sensorDS.filter(sensor->"s1".equals(sensor.getId()));



        filter.print();


        env.execute();
    }


}
