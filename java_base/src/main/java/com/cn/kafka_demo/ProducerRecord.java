package com.cn.kafka_demo;

import io.netty.handler.codec.Headers;

public class ProducerRecord<K, V> {

    private final String topic;
    private final Integer partition;
    private final Headers headers;
    private final K key;
    private final V value;
    private final Long timestamp;

    public ProducerRecord(String topic, Integer partition, Headers headers, K key, V value, Long timestamp) {
        this.topic = topic;
        this.partition = partition;
        this.headers = headers;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }
}

