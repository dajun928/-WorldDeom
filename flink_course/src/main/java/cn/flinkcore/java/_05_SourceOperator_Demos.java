package cn.flinkcore.java;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public class _05_SourceOperator_Demos {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);  // Ĭ�ϲ��ж�

        /**
         * �Ӽ��ϵõ�������
         */
//        DataStreamSource<Integer> fromElements = env.fromElements(1, 2, 3, 4, 5);
//        fromElements.map(d -> d * 10)/*.print()*/;
//
//        List<String> dataList = Arrays.asList("a", "b", "a", "c");
//        // fromCollection���������ص�source���ӣ���һ�������жȵ�source����
//        DataStreamSource<String> fromCollection = env.fromCollection(dataList)/*.setParallelism(5)*/;  // ���������������ʽ����>1�Ĳ��жȣ������쳣
//        fromCollection.map(String::toUpperCase).print();


        // fromParallelCollection�����ص�source���ӣ���һ���ಢ�жȵ�source����
//        DataStreamSource<LongValue> parallelCollection = env.fromParallelCollection(new LongValueSequenceIterator(1, 100), TypeInformation.of(LongValue.class)).setParallelism(2);
//        parallelCollection.map(lv -> lv.getValue() + 100)/*.print()*/;
//
//        DataStreamSource<Long> sequence = env.generateSequence(1, 100);
//        sequence.map(x -> x - 1)/*.print()*/;

        /**
         * �� socket �˿ڻ�ȡ���ݵõ�������
         * socketTextStream����������source���ӣ���һ�������жȵ�source����
         */
        // DataStreamSource<String> socketSource = env.socketTextStream("localhost", 9999);
        // socketSource.print();


        /**
         * ���ļ��õ�������
         */
//        DataStreamSource<String> fileSource = env.readTextFile("flink_course/data/wc/input/wc.txt", "utf-8");
//        fileSource.map(String::toUpperCase)/*.print()*/;
//
//
//        // FileProcessingMode.PROCESS_ONCE  ��ʾ�����ļ�ֻ��һ�Σ�����һ�Σ�Ȼ�������˳�
//        // FileProcessingMode.PROCESS_CONTINUOUSLY ��ʾ����������ļ��ı仯��һ�������ļ��б仯������ٴζ������ļ��������¼���
//        DataStreamSource<String> fileSource2 = env.readFile(new TextInputFormat(null), "flink_course/data/wc/input/wc.txt", FileProcessingMode.PROCESS_CONTINUOUSLY, 1000);
//        fileSource2.map(String::toUpperCase)/*.print()*/;


        /**
         * ������չ�� ��  flink-connector-kafka
         * ��kafka�ж�ȡ���ݵõ�������
         */
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                // ���ö��ĵ�Ŀ������s
                .setTopics("topic_test")

                // ������������id
                .setGroupId("gp01")

                // ����kafka��������ַ
                .setBootstrapServers("192.168.43.112:9092")

                // ��ʼ����λ�Ƶ�ָ����
                //    OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST) ������ʼλ��ѡ��֮ǰ���ύ��ƫ���������û�У�������ΪLATEST��
                //    OffsetsInitializer.earliest()  ������ʼλ��ֱ��ѡ��Ϊ �����硱
                //    OffsetsInitializer.latest()  ������ʼλ��ֱ��ѡ��Ϊ �����¡�
                //    OffsetsInitializer.offsets(Map<TopicPartition,Long>)  ������ʼλ��ѡ��Ϊ�������������ÿ�������Ͷ�Ӧ����ʼƫ����
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))

                // ����value���ݵķ����л���
                .setValueOnlyDeserializer(new SimpleStringSchema())

                // ����kafka�ײ������ߵ��Զ�λ���ύ����
                //    ��������µ�����λ���ύ��kafka��consumer_offsets��
                //    ������Զ�λ���ύ���ƿ�����KafkaSource��Ȼ�������Զ�λ���ύ����
                //    ��崻�����ʱ�����ȴ�flink�Լ���״̬��ȥ��ȡƫ����<���ɿ�>��
                .setProperty("auto.offset.commit", "true")

                // �ѱ�source�������ó�  BOUNDED���ԣ��н�����
                //     ������sourceȥ��ȡ���ݵ�ʱ�򣬶���ָ����λ�ã���ֹͣ��ȡ���˳�
                //     �����ڲ�����������ĳһ����ʷ����
                // .setBounded(OffsetsInitializer.committedOffsets())

                // �ѱ�source�������ó�  UNBOUNDED���ԣ��޽�����
                //     ���ǲ�����һֱ�����ݣ����Ǵﵽָ��λ�þ�ֹͣ��ȡ���������˳�
                //     ��ҪӦ�ó�������Ҫ��kafka�ж�ȡĳһ�ι̶����ȵ����ݣ�Ȼ�������������ȥ������һ���������޽������ϴ���
                //.setUnbounded(OffsetsInitializer.latest())

                .build();

//        env.addSource();  //  ���յ���  SourceFunction�ӿڵ� ʵ����
        DataStreamSource<String> streamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kfk-source");//  ���յ��� Source �ӿڵ�ʵ����
        streamSource.print();


        env.execute();


    }
}
