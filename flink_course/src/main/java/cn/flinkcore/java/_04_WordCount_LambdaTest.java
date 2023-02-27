package cn.flinkcore.java;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class _04_WordCount_LambdaTest {

    public static void main(String[] args) throws Exception {

        // ����һ�������ڣ�ִ�л�����

        // ��ʽ������ڻ���
        StreamExecutionEnvironment envStream = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource = envStream.readTextFile("flink_course/src/main/java/cn/flink/data/wc.txt");

        // �ȰѾ��ӱ��д
        /* ��map���ӽ��յ�MapFunction�ӿ�ʵ������������һ�������󷽷��Ľӿ�
        ��������ӿڵ�ʵ����ĺ��Ĺ��ܣ��������ķ�����
        �ǾͿ�����lambda���ʽ�����ʵ��
        streamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return null;
            }
        });*/

        /**
         * lambda���ʽ��ôд������Ҫʵ�ֵ��Ǹ��ӿڵķ�������ʲô����������ʲô���
         */
        // Ȼ��Ͱ�lambda�﷨����  (����1,����2,...) -> { ������ }
        // streamSource.map( (value) -> { return  value.toUpperCase();});

        // ���������lambda���ʽ�������б�ֻ��һ�����Һ�����ֻ��һ�д��룬����Լ�
        // streamSource.map( value ->  value.toUpperCase() ) ;

        // ���������lambda���ʽ�� ������ֻ��һ�д��룬�Ҳ���ֻʹ����һ�Σ����԰Ѻ�������ת��  ���������á�
        SingleOutputStreamOperator<String> upperCased = streamSource.map(String::toUpperCase);

        // Ȼ���гɵ��ʣ���ת�ɣ�����,1������ѹƽ
        /*upperCased.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {

            }
        });*/
        // ������Ľӿ�����������Ȼ��һ��   �����󷽷��� �ӿڣ��������ķ���ʵ�֣���Ȼ������lambda���ʽ��ʵ��
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = upperCased.flatMap((String s, Collector<Tuple2<String, Integer>> collector) -> {
                    String[] words = s.split("\\s+");
                    for (String word : words) {
                        collector.collect(Tuple2.of(word, 1));
                    }
                })
                // .returns(new TypeHint<Tuple2<String, Integer>>() {});   // ͨ�� TypeHint ���ﷵ����������
                // .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));  // ��ͨ�õģ��Ǵ���TypeInformation,�����TypeHintҲ�Ƿ�װ��TypeInformation
                .returns(Types.TUPLE(Types.STRING, Types.INT));  // ���ù�����Types�ĸ��־�̬������������TypeInformation


        // �����ʷ���
        /*wordAndOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return null;
            }
        })*/
        // �������KeySelector�ӿ�����������Ȼ��һ�� �����󷽷��� �ӿڣ��������ķ���ʵ�֣���Ȼ������lambda���ʽ��ʵ��
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordAndOne.keyBy((value) -> value.f0);


        // ͳ�Ƶ��ʸ���
        keyedStream.sum(1)
                .print();


        envStream.execute();


    }
}

