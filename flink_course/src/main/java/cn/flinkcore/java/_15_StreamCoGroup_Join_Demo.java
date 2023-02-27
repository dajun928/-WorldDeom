package cn.flinkcore.java;


import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @Author: deep as the sea
 * @Site: www.51doit.com
 * @QQ: 657270652
 * @Date: 2022/4/26
 * @Desc: coGroupЭͬ�������Ӵ���ʾ��
 **/
public class _15_StreamCoGroup_Join_Demo {


    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);

        // id,name
        DataStreamSource<String> stream1 = env.socketTextStream("localhost", 9998);

        SingleOutputStreamOperator<Tuple2<String, String>> s1 = stream1.map(s -> {
            String[] arr = s.split(",");
            return Tuple2.of(arr[0], arr[1]);
        }).returns(new TypeHint<Tuple2<String, String>>() {
        });

        // id,age,city
        DataStreamSource<String> stream2 = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple3<String, String, String>> s2 = stream2.map(s -> {
            String[] arr = s.split(",");
            return Tuple3.of(arr[0], arr[1], arr[2]);
        }).returns(new TypeHint<Tuple3<String, String, String>>() {
        });

        /**
         * ���� cogroup
         * ����������
         *    ��1���ݣ�  id,name
         *    ��2���ݣ�  id,age,city
         *    ����coGroup���ӣ���ʵ�������������ݰ�id��Ƚ��д��ڹ���������inner ��left�� right�� outer��
         */
        DataStream<String> resultStream = s1.coGroup(s2)
                .where(tp -> tp.f0)  // ������  f0 �ֶ�
                .equalTo(tp -> tp.f0)   // ������ f0 �ֶ�
                .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))  // ���ִ���
                .apply(new CoGroupFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>() {
                    /**
                     *
                     * @param first  ��Эͬ���еĵ�һ����������
                     * @param second ��Эͬ���еĵڶ�����������
                     * @param out �Ǵ������������
                     * @throws Exception
                     */
                    @Override
                    public void coGroup(Iterable<Tuple2<String, String>> first, Iterable<Tuple3<String, String, String>> second, Collector<String> out) throws Exception {

                        // ������ʵ��  left out  join
                        for (Tuple2<String, String> t1 : first) {
                            boolean flag = false;
                            for (Tuple3<String, String, String> t2 : second) {
                                // ƴ�������ֶ����
                                out.collect(t1.f0 + "," + t1.f1 + "," + t2.f0 + "," + t2.f1 + "," + t2.f2);
                                flag = true;
                            }

                            if (!flag) {
                                // ������ߵ������棬˵���ұ�û�����ݣ���ֱ������������
                                out.collect(t1.f0 + "," + t1.f1 + "," + null + "," + null + "," + null);
                            }
                        }

                        // TODO  ʵ������ right out join


                        // TODO ʵ��  full out join


                        // TODO  ʵ��  inner join


                    }
                });
        /*resultStream.print();*/

        /**
         * ���� join ����
         * ����������
         *    ��1���ݣ�  id,name
         *    ��2���ݣ�  id,age,city
         *    ����join���ӣ���ʵ�������������ݰ�id����
         */
        DataStream<String> joinedStream = s1.join(s2)
                .where(tp2 -> tp2.f0)
                .equalTo(tp3 -> tp3.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
                .apply(new JoinFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>() {
                    @Override
                    public String join(Tuple2<String, String> t1, Tuple3<String, String, String> t2) throws Exception {
                        return t1.f0 + "," + t1.f1 + "," + t2.f0 + "," + t2.f1 + "," + t2.f2;
                    }
                });

        joinedStream.print();



        env.execute();

    }


}

