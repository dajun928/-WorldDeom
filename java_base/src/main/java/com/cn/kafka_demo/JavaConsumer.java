package com.cn.kafka_demo;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.data.redis.connection.DefaultStringRedisConnection;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class JavaConsumer {

    public static final String bootstrapServer = "192.168.43.112:9092";
    public static final String topic = "kafka_demo";
    public static final String group_id = "test-group2";

    public static final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("bootstrap.servers",bootstrapServer);
        properties.put("group.id",group_id);
        // 创建消费者客户端
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        // 订阅主题
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        // 循环消费消息
        try{
            while (isRunning.get()){
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    // 处理consumerRecord
                    System.out.println(consumerRecord);
                }
            }
        }catch (Exception e){
            // 处理异常
        }finally {
            kafkaConsumer.close();
        }

    }
}
