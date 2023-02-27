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
 * 通过socket数据源，去请求一个socket服务（doit01:9999）得到数据流
 * 然后统计数据流中出现的单词及其个数 Java Lambda 表达式实现
 */
public class MyFirstFlinkDemo {
    public static void main(String[] args) throws Exception {
        //LocalStreamEnvironment只能在local模式运行，通常用于本地测试
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(8);

        DataStreamSource<String> lines = env.socketTextStream("192.168.43.132", 9999);

        //使用java8的Lambda表达式
        //使用Lambda表达式，要有return返回信息
        SingleOutputStreamOperator<String> words = lines.flatMap((String line, Collector<String> out) -> Arrays.stream(line.split(" ")).forEach(out::collect)).returns(Types.STRING);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = words.map(w -> Tuple2.of(w, 1)).returns(Types.TUPLE(Types.STRING, Types.INT));

        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = wordAndOne.keyBy(0).sum(1);

        //打印输出
        summed.print();

        //抛出异常
        env.execute();

    }
}
