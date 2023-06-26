package cn.flinkcore.java;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * ͨ��socket����Դ��ȥ����һ��socket����192.168.43.132:9999���õ�������
 * Ȼ��ͳ���������г��ֵĵ��ʼ������ java ����ʵ��
 * windows nc -l -p 9999
 * input  java scala php c++ java scala php java scala java
 * output
 * 10> (php,1)
 * 4> (java,1)
 * 9> (c++,1)
 * 1> (scala,1)
 * 10> (php,2)
 * 4> (java,2)
 * 1> (scala,2)
 * 4> (java,3)
 * 1> (scala,3)
 * 4> (java,4)
 */
public class _01_StreamWordCount {

    public static void main(String[] args) throws Exception {
        // ����һ�������ڻ���
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();  // ����һ�����ڻ���

        // ͨ��source���ӣ���socket����Դ����Ϊһ��dataStream����������
//        SingleOutputStreamOperator<String> source = env.socketTextStream("192.168.43.132", 9999);
        SingleOutputStreamOperator<String> source = env.socketTextStream("localhost", 9999);

        // Ȼ��ͨ�����Ӷ����������и���ת���������߼���
        SingleOutputStreamOperator<Tuple2<String, Integer>> words = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                // �е���
                String[] split = s.split("\\s+");
                for (String word : split) {
                    // ����ÿһ��  (����,1)
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyed = words.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.f0;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = keyed.sum("f1");

        // ͨ��sink���ӣ���������
        resultStream.print();

        // ����������ύ����
        env.execute();
    }
}

