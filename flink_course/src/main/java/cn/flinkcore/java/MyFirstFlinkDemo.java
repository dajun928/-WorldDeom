package cn.flinkcore.java;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;
/**
 * ͨ��socket����Դ��ȥ����һ��socket����doit01:9999���õ�������
 * Ȼ��ͳ���������г��ֵĵ��ʼ������ Java Lambda ���ʽʵ��
 */
public class MyFirstFlinkDemo {
    public static void main(String[] args) throws Exception {
        //LocalStreamEnvironmentֻ����localģʽ���У�ͨ�����ڱ��ز���
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(8);

        DataStreamSource<String> lines = env.socketTextStream("192.168.43.132", 9999);

        //ʹ��java8��Lambda���ʽ
        //ʹ��Lambda���ʽ��Ҫ��return������Ϣ
        SingleOutputStreamOperator<String> words = lines.flatMap((String line, Collector<String> out) -> Arrays.stream(line.split(" ")).forEach(out::collect)).returns(Types.STRING);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = words.map(w -> Tuple2.of(w, 1)).returns(Types.TUPLE(Types.STRING, Types.INT));

        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = wordAndOne.keyBy(0).sum(1);

        //��ӡ���
        summed.print();

        //�׳��쳣
        env.execute();

    }
}
