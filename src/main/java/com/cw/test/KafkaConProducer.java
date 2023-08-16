package com.cw.test;

import com.cw.utils.YamlUtil;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 多线程下生产者
 **/
public class KafkaConProducer {
    //发送消息个数
    private static final int MSG_SIZE = 1000;
    //负责发送消息的线程池
    private static ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private static CountDownLatch countDownLatch = new CountDownLatch(MSG_SIZE);

    private static class ProducerWorker implements Runnable {
        private ProducerRecord<String, String> record;
        private KafkaProducer<String, String> producer;

        public ProducerWorker(ProducerRecord<String, String> record, KafkaProducer<String, String> producer) {
            this.record = record;
            this.producer = producer;
        }
        @Override
        public void run() {
            final String id = Thread.currentThread().getId() + "-" + System.identityHashCode(producer);
            try {
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            e.printStackTrace();
                        }
                        if (recordMetadata != null) {
                            System.out.println("offset:" + recordMetadata.offset() + ";partition:" + recordMetadata.partition());
                        }
                    }
                });
                System.out.println(id + "：数据[" + record + "]已发送。");
                countDownLatch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args)  {
        String kfkServers = YamlUtil.getValueByKey("kafka-conf.yml","kafka","servers");
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kfkServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        try {
            for (int i = 0; i < MSG_SIZE; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(
                        "ods-test", null, System.currentTimeMillis(), String.valueOf(i), "Fisher" + i);
                executorService.submit(new ProducerWorker(record, producer));
            }
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            producer.close();
            executorService.shutdown();
        }
    }
}

