package com.cn.kafka_demo;

import io.netty.handler.codec.Headers;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;

import java.util.Optional;

public class ConsumerRecord<K, V> {
    public static final long NO_TIMESTAMP = RecordBatch.NO_TIMESTAMP;
    public static final int NULL_SIZE = -1;

    /**
     * @deprecated checksums are no longer exposed by this class, this constant will be removed in Apache Kafka 4.0
     *             (deprecated since 3.0).
     */
    @Deprecated
    public static final int NULL_CHECKSUM = -1;

    private final String topic;
    private final int partition;
    private final long offset;
    private final long timestamp;
    private final TimestampType timestampType;
    private final int serializedKeySize;
    private final int serializedValueSize;
    private final Headers headers;
    private final K key;
    private final V value;
    private final Optional<Integer> leaderEpoch;


    public ConsumerRecord(String topic, int partition, long offset, long timestamp, TimestampType timestampType, int serializedKeySize, int serializedValueSize, Headers headers, K key, V value, Optional<Integer> leaderEpoch) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.timestampType = timestampType;
        this.serializedKeySize = serializedKeySize;
        this.serializedValueSize = serializedValueSize;
        this.headers = headers;
        this.key = key;
        this.value = value;
        this.leaderEpoch = leaderEpoch;
    }
}

