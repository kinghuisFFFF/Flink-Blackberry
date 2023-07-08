package com.cw.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * description
 * created by A on 2021/3/19
 */

/**
 * 注意：
 *      1.CK超时时间不能大于Kafka的超时时间，否则会报错
 *      2.在生产环境中，如果要开启kafka精准一次消费，必须开启CK
 */
public class MyKafkaUtil {

//    private static String KAFKA_SERVER = "hadoop366:9092";
    private static String KAFKA_SERVER = "hadoop32:9092";
    //准备配置信息
    private static Properties properties=new Properties();

    //指定DWD事实数据默认主题
    private static final String DWD_DEFAULT_TOPIC = "dwd_default_topic";

    static{
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop32:9092");
    }

    /**
     *  获取kafka生产者
     * @param topic 写入kafka的主题名
     * @return
     */
    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {

        //创建Kafka生产者对象并返回
        return new FlinkKafkaProducer<String>(topic,
                new SimpleStringSchema(),
                properties);
    }

    /**
     *
     * @param kafkaSchema 指定kafka序列化器
     * @param <T>
     * @return
     */
    public static <T> FlinkKafkaProducer<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> kafkaSchema){
//        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));
//        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(1200000L)); // 2min ,ck 超时时间1min
        return new FlinkKafkaProducer<T>(
                //指定默认主题
            DWD_DEFAULT_TOPIC,
                //指定序列化器，这里使用接口的形式实现多态
                kafkaSchema,
                //指定配置文件
                properties,
                //设置精准一次性消费
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }

    public static <T> FlinkKafkaProducer<T> getKafkaSinkBySchema2(KafkaSerializationSchema<T> kafkaSchema){
//        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));
//        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(1200000L)); // 2min ,ck 超时时间1min
        return new FlinkKafkaProducer<T>(
                //指定默认主题
                DWD_DEFAULT_TOPIC,
                //指定序列化器，这里使用接口的形式实现多态
                kafkaSchema,
                //指定配置文件
                properties,
                //设置精准一次性消费,EXACTLY_ONCE 报错，故修改为NONE
                FlinkKafkaProducer.Semantic.NONE
        );
    }

    public static <T> FlinkKafkaProducer<T> getKafkaProducer(KafkaSerializationSchema<T> kafkaSchema){
//        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));
//        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(1200000L)); // 2min ,ck 超时时间1min
        return new FlinkKafkaProducer<T>(
                //指定默认主题
                DWD_DEFAULT_TOPIC,
                //指定序列化器，这里使用接口的形式实现多态
                kafkaSchema,
                //指定配置文件
                properties,
                //设置精准一次性消费后,flink任务必须开启检查点,否则此配置不生效
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }

    /**
     * 获取Kafka的消费者
     *
     * @param groupId 消费者组
     * @param topic   消费的主题
     * @return
     */
    public static FlinkKafkaConsumer<String> getKafkaSource(String groupId, String topic) {
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
//        earliest latest none
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false)); // 手动提交偏移量
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);
    }


    /**
     *
     * @param topic 消费的主题
     * @param groupId 消费者组
     * @return
     */
    public static FlinkKafkaConsumer<String> getKafkaSource2(String topic, String groupId) {
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
//        earliest latest none
//        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false)); // 手动提交偏移量
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);
    }

// latest-offset   earliest-offset
    //拼接Kafka相关属性到DDL
    public static String getKafkaDDL(String topic, String groupId) {
        return "'connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + KAFKA_SERVER + "', " +
                " 'properties.group.id' = '" + groupId + "', " +
                "  'format' = 'json', " +
                "  'scan.startup.mode' = 'latest-offset'";
    }


    //拼接Kafka相关属性到DDL
    public static String getKafkaDDL2(String topic, String groupId) {
        return "'connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + KAFKA_SERVER + "', " +
                " 'properties.group.id' = '" + groupId + "', " +
                "  'format' = 'json', " +
                "  'scan.startup.mode' = 'earliest-offset'";
    }
}
