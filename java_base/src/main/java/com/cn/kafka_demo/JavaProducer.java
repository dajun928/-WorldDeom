package com.cn.kafka_demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class JavaProducer{

    // kafka��ַ
    public static final String bootstrapServer = "192.168.43.112:9092";
    // topic����
    public static final String topic = "kafka_demo";

    public static void main(String[] args) {

        Properties properties = new Properties();
        // ָ��key����Ϣ��value�ı��뷽ʽ
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("bootstrap.servers",bootstrapServer);

        // ����������������
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        // ������Ϣ����ָ������
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic,"test message 035");
        // ������Ϣ
        kafkaProducer.send(producerRecord);
        // �ر������߿ͻ���
        kafkaProducer.close();

    }

}
