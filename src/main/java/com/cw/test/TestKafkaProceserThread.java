package com.cw.test;

import com.cw.app.func.MyPartitioner;
import com.cw.utils.YamlUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TestKafkaProceserThread {
    //Kafka配置文件
    public static final String TOPIC_NAME = "ods_test";
    public static final String KAFKA_PRODUCER = "kafka-producer.properties";
    public static final int producerNum=50;//实例池大小
    //阻塞队列实现生产者实例池,获取连接作出队操作，归还连接作入队操作
    public static BlockingQueue<KafkaProducer<String, String>> queue=new LinkedBlockingQueue<>(producerNum);
    //初始化producer实例池
    static {
        for (int i = 0; i <producerNum ; i++) {
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
            KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
            queue.add(kafkaProducer);
        }
    }
    //生产者发送线程
    static class SendThread extends Thread {
        String msg;
        public SendThread(String msg){
            this.msg=msg;
        }
        @Override
        public void run(){
            ProducerRecord record = new ProducerRecord(TOPIC_NAME, msg);
            try {
                KafkaProducer<String, String> kafkaProducer =queue.take();//从实例池获取连接,没有空闲连接则阻塞等待
                kafkaProducer.send(record);
                queue.put(kafkaProducer);//归还kafka连接到连接池队列
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    //测试
    public static void main(String[]args){
        for (int i = 0; i <100 ; i++) {
            SendThread sendThread=new SendThread("test multi-thread producer!");
            sendThread.start();
        } 
    }
}
