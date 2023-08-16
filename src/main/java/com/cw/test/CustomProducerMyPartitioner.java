package com.cw.test;

import com.cw.app.func.MyPartitioner;
import com.cw.utils.YamlUtil;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileNotFoundException;
import java.util.Properties;

/**
 * @author: xz
 * @since: 2023/4/9 20:56
 * @description: 使用自定义的分区器方法，在生产者的配置中添加分区器参数。
 */
public class CustomProducerMyPartitioner {
    public static void main(String[] args) throws FileNotFoundException, InterruptedException {
        String kfkServers = YamlUtil.getValueByKey("kafka-conf.yml","kafka","servers");
        //1、创建 kafka 生产者的配置对象
        Properties properties = new Properties();

        //2、给 kafka 配置对象添加配置信息：bootstrap.servers
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,kfkServers);

        //3、指定对应的key和value的序列化类型 key.serializer value.serializer
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        
        //4、添加自定义分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyPartitioner.class);
        
        //5、创建 kafka 生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        //6、调用 send 方法,发送消息
        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<>("ods_news", "hello kafka" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null){
                        System.out.println("主题： "+metadata.topic() + " 分区： "+ metadata.partition());
                    }
                }
            });
            Thread.sleep(2);
        }

        //7、关闭资源
        kafkaProducer.close();
    }
}
