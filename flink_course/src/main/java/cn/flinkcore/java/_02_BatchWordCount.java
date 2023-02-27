package cn.flinkcore.java;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Author: deep as the sea
 * @Desc: ���������ģʽ��wordcountʾ��
 **/
public class _02_BatchWordCount {

    public static void main(String[] args) throws Exception {

        // ��������ڻ���
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();

        // ������  -- : �������еõ������ݳ�����һ�� DataSet
        DataSource<String> stringDataSource = batchEnv.readTextFile("flink_course/src/main/java/cn/flink/data/");

        // ��dataset�ϵ��ø���dataset������
        stringDataSource
                .flatMap(new MyFlatMapFunction())
                .groupBy(0)
                .sum(1)
                .print();
    }
}

class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String,Integer>>{

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
        String[] words = value.split("\\s+");
        for (String word : words) {
            out.collect(Tuple2.of(word,1));
        }
    }
}
