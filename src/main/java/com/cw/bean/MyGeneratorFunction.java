package com.cw.bean;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

/**
 * @Title:
 * @BelongProjecet Flink-Blackberry
 * @BelongPackage com.cw.bean
 * @Description:
 * @Copyright time company - Powered By 研发一部
 * @Author: cw
 * @Date: 2023/6/8 14:29
 * @Version V1.0
 */
public class MyGeneratorFunction implements GeneratorFunction<Long, TrafficData2>{
    /** 随机数据生成器对象 */
    /** 随机数据生成器对象 */
    public org.apache.commons.math3.random.RandomDataGenerator generator;

    @Override
    public void open(SourceReaderContext readerContext) throws Exception {
        generator = new org.apache.commons.math3.random.RandomDataGenerator();
    }

    @Override
    public TrafficData2 map(Long randomDataGenerator) throws Exception {
        // 使用随机生成器生成数据，构造流量对象
        return new TrafficData2(
                generator.nextInt(1, 100),
                generator.nextInt(1, 10),
                System.currentTimeMillis(),
                generator.nextUniform(0, 1)
        );
    }

}
