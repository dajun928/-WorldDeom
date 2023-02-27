package cn.flinkcore.java;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @Author: deep as the sea
 * @Site: www.51doit.com
 * @QQ: 657270652
 * @Date: 2022/4/26
 * @Desc: ��������connect����  ��   ���Ĺ���join����  ����ʾ��
 **/
public class _14_StreamConnect_Union_Demo {


    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);

        // �����ַ���
        DataStreamSource<String> stream1 = env.socketTextStream("localhost", 9998);

        // ��ĸ�ַ���
        DataStreamSource<String> stream2 = env.socketTextStream("localhost", 9999);

        /**
         * ���� connect
         */
        ConnectedStreams<String, String> connectedStreams = stream1.connect(stream2);

        SingleOutputStreamOperator<String> resultStream = connectedStreams.map(new CoMapFunction<String, String, String>() {
            // ��ͬ��״̬����

            String  prefix = "doitedu_";

            /**
             * �� ���� ������߼�
             * @param value
             * @return
             * @throws Exception
             */
            @Override
            public String map1(String value) throws Exception {
                // ������*10���ٷ����ַ���
                return  prefix + (Integer.parseInt(value)*10) + "";
            }

            /**
             * �� ���� ������߼�
             * @param value
             * @return
             * @throws Exception
             */
            @Override
            public String map2(String value) throws Exception {

                return prefix + value.toUpperCase();
            }
        });
        /*resultStream.print();*/


        /**
         * ���� union
         * ���� union������������������һ��
         */
        // stream1.map(Integer::parseInt).union(stream2); // union�������ߵ������Ͳ�һ�£���ͨ��
        DataStream<String> unioned = stream1.union(stream2);
        unioned.map(s-> "doitedu_"+s).print();


        env.execute();

    }


}

