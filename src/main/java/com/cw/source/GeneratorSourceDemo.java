package com.cw.source;

import com.cw.bean.TrafficData;
import com.cw.utils.FilnkUtils;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;

/**
 * 使用数据生成器
 */
public class GeneratorSourceDemo {
    public static void main(String[] args) throws Exception {
        // 创建env对象
        StreamExecutionEnvironment env = FilnkUtils.getStreamExecutionEnvironmentDev();
        // 创建DataGeneratorSource，传入上面自定义的数据生成器
        DataGeneratorSource<TrafficData> trafficDataDataGeneratorSource = new DataGeneratorSource<>(new TrafficData.TrafficDataGenerator());

        // 添加source
        env.addSource(trafficDataDataGeneratorSource)
                // 指定返回类型
                .returns(new TypeHint<TrafficData>() {
                })
                // 输出
                .print();
        env.execute();
    }
}
