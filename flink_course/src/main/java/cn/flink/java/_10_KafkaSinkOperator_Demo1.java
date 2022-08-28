package cn.flink.java;


import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *
 * @Author: deep as the sea
 * @Site: www.51doit.com
 * @QQ: 657270652
 * @Date: 2022/4/26
 * @Desc:
 *   ����KafkaSink��������д��kafka
 *   ����׼��������Ŀ��topic��
 *   [root@doit01 ~]# kafka-topics.sh --create --topic event-log --partitions 3 --replication-factor 2 --zookeeper doit01:2181
 **/
public class _10_KafkaSinkOperator_Demo1 {
    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port",8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);


        // ����checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");

        // �����һ��������
        DataStreamSource<EventLog> streamSource = env.addSource(new MySourceFunction());


        // ������д��kafka
        // 1. ����һ��kafka��sink����
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("192.168.43.112:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setTopic("event-log3")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setTransactionalIdPrefix("doitedu-")
                .build();

        // 2. �����������������õ�sink����
        streamSource
                .map(JSON::toJSONString).disableChaining()
                .sinkTo(kafkaSink);

        env.execute();
    }
}

