package com.cw.sink;

import com.cw.bean.MyGeneratorFunction;
import com.cw.bean.TrafficData2;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 */
public class SinkKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 如果是精准一次，必须开启checkpoint（后续章节介绍）
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);


        SingleOutputStreamOperator<String> sensorDS = env
                .socketTextStream("tencentcloud.yawujia.cn", 8082);
        // 创建DataGeneratorSource，传入上面自定义的数据生成器
        DataGeneratorSource<TrafficData2> TrafficData2DataGeneratorSource = new DataGeneratorSource(new MyGeneratorFunction(), Long.MAX_VALUE, Types.POJO(TrafficData2.class));


        // 添加source
        SingleOutputStreamOperator<TrafficData2> ds1 = env.fromSource(TrafficData2DataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator")
                // 指定返回类型
                .returns(new TypeHint<TrafficData2>() {
                });
        SingleOutputStreamOperator<String> ds2 = ds1.map(line -> line.toString());

        /**
         * Kafka Sink:
         * TODO 注意：如果要使用 精准一次 写入Kafka，需要满足以下条件，缺一不可
         * 1、开启checkpoint（后续介绍）
         * 2、设置事务前缀
         * 3、设置事务超时时间：   checkpoint间隔 <  事务超时时间  < max的15分钟
         */
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                // 指定 kafka 的地址和端口
//                .setBootstrapServers("tencentcloud.yawujia.cn:9092,hadoop103:9092,hadoop104:9092")
                .setBootstrapServers("kafka:9092")
                // 指定序列化器：指定Topic名称、具体的序列化
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic("ws")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                // 写到kafka的一致性级别： 精准一次、至少一次
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                // 如果是精准一次，必须设置 事务的前缀
                .setTransactionalIdPrefix("atcw-")
                // 如果是精准一次，必须设置 事务超时时间: 大于checkpoint间隔，小于 max 15分钟
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10*60*1000+"")
                .build();


//        sensorDS.sinkTo(kafkaSink);
        ds2.sinkTo(kafkaSink);


        env.execute();
    }
}
