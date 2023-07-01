package com.cw.source;

import com.cw.bean.MyGeneratorFunction;
import com.cw.bean.TrafficData2;
import com.cw.utils.FilnkUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 使用数据生成器
 */
public class GeneratorSourceDemo2 {
    public static void main(String[] args) throws Exception {
        // 创建env对象
        StreamExecutionEnvironment env = FilnkUtils.getStreamExecutionEnvironmentDev();
        // 创建DataGeneratorSource，传入上面自定义的数据生成器
        DataGeneratorSource<TrafficData2> TrafficData2DataGeneratorSource = new DataGeneratorSource(new MyGeneratorFunction(), Long.MAX_VALUE, Types.POJO(TrafficData2.class));


        // 添加source
        env.fromSource(TrafficData2DataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator")
                // 指定返回类型
                .returns(new TypeHint<TrafficData2>() {
                })
                // 输出
                .print();
        env.execute();
    }
}
