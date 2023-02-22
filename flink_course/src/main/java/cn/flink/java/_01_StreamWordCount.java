package cn.flink.java;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 通过socket数据源，去请求一个socket服务（doit01:9999）得到数据流
 * 然后统计数据流中出现的单词及其个数 java 语言实现
 */
public class _01_StreamWordCount {

    public static void main(String[] args) throws Exception {
        // 创建一个编程入口环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();  // 流批一体的入口环境

        // 通过source算子，把socket数据源加载为一个dataStream（数据流）
        SingleOutputStreamOperator<String> source = env.socketTextStream("192.168.43.132", 9999);

        // 然后通过算子对数据流进行各种转换（计算逻辑）
        SingleOutputStreamOperator<Tuple2<String, Integer>> words = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                // 切单词
                String[] split = s.split("\\s+");
                for (String word : split) {
                    // 返回每一对  (单词,1)
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

        // 通过sink算子，将结果输出
        resultStream.print();

        // 触发程序的提交运行
        env.execute();


    }
}

