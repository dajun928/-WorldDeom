package com.cn.kafka_demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class JavaProducer{

    // kafka地址
    public static final String bootstrapServer = "192.168.43.112:9092";
    // topic主题
    public static final String topic = "kafka_demo";

    public static void main(String[] args) {

        Properties properties = new Properties();
        // 指定key和消息体value的编码方式
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("bootstrap.servers",bootstrapServer);

        // 创建并配置生产者
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        // 创建消息，并指定分区
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic,"test message 035");
        // 发送消息
        kafkaProducer.send(producerRecord);
        // 关闭生产者客户端
        kafkaProducer.close();

    }

}
