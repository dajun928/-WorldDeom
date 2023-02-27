package cn.flinkcore.java;


import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlinkHelloWorld {

    public static void main(String[] args) throws Exception {
        // 1.׼������
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // ��������ģʽ
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // 2.��������Դ
        DataStreamSource<String> elementsSource = env.socketTextStream("192.168.43.132", 9999);
        // 3.����ת��
        DataStream<String> flatMap = elementsSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String element, Collector<String> out) {
                String[] wordArr = element.split(",");
                for (String word : wordArr) {
                    out.collect(word);
                }
            }
        });
        //DataStream �±�ΪDataStream����
        SingleOutputStreamOperator<String> source = flatMap.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) {
                return value.toUpperCase();
            }
        });
        // 4.�������
        source.print();
        // 5.ִ�г���
        env.execute("flink-hello-world");
    }
}

