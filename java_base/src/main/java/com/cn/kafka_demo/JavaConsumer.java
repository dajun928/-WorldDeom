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
        // ���������߿ͻ���
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        // ��������
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        // ѭ��������Ϣ
        try{
            while (isRunning.get()){
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    // ����consumerRecord
                    System.out.println(consumerRecord);
                }
            }
        }catch (Exception e){
            // �����쳣
        }finally {
            kafkaConsumer.close();
        }

    }
}
