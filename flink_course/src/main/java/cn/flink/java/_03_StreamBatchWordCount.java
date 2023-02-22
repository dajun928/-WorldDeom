package cn.flink.java;


import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class _03_StreamBatchWordCount {

    public static void main(String[] args) throws Exception {

        // ������ı�̻������
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        // ��������ģʽȥִ��
        streamEnv.setRuntimeMode(RuntimeExecutionMode.BATCH);

        // ��������ģʽȥִ��
        // streamEnv.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        // flink�Լ��жϾ���
        // streamEnv.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // ���ļ� �õ�  dataStream
        DataStreamSource<String> streamSource = streamEnv.readTextFile("flink_course/src/main/java/cn/flink/data/wc.txt");


        // ����dataStream������������
        streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] words = value.split("\\s+");
                        for (String word : words) {
                            out.collect(Tuple2.of(word, 1));
                        }
                    }
                })
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                .sum(1)
                .print();


        streamEnv.execute();

    }

}

