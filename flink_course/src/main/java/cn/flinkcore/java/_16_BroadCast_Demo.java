package cn.flinkcore.java;


import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author: deep as the sea
 * @Site: www.51doit.com
 * @QQ: 657270652
 * @Date: 2022/4/26
 * @Desc: �㲥�����㲥״̬��ʹ��ʾ��
 **/
public class _16_BroadCast_Demo {


    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);

        // id,eventId
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
         * ����������
         *    �� 1��  �û���Ϊ�¼������������ϣ�ͬһ����Ҳ�ᷴ�����֣����ִ�������
         *    �� 2��  �û�ά����Ϣ�����䣬���У���ͬһ���˵�����ֻ����һ�Σ�����ʱ��Ҳ���� ����Ϊ�㲥����
         *
         *    ��Ҫ�ӹ���1�����û���ά����Ϣ���ã����ù㲥����ʵ��
         */

        // ���ֵ������������� s2  ��  ת�� �㲥��
        MapStateDescriptor<String, Tuple2<String, String>> userInfoStateDesc = new MapStateDescriptor<>("userInfoStateDesc", TypeInformation.of(String.class), TypeInformation.of(new TypeHint<Tuple2<String, String>>() {}));
        BroadcastStream<Tuple3<String, String, String>> s2BroadcastStream = s2.broadcast(userInfoStateDesc);

        // �ĸ�����������Ҫ�õ��㲥״̬���ݣ���Ҫ ȥ  ���� connect  ����㲥��
        BroadcastConnectedStream<Tuple2<String, String>, Tuple3<String, String, String>> connected = s1.connect(s2BroadcastStream);


        /**
         *   �� �����˹㲥��֮��� ���������� ���д���
         *   ����˼�룺
         *      ��processBroadcastElement�����У��ѻ�ȡ���Ĺ㲥���е����ݣ����뵽 ���㲥״̬����
         *      ��processElement�����У���ȡ�����������ݽ��д����ӹ㲥״̬�л�ȡҪƴ�ӵ����ݣ�ƴ�Ӻ������
         */
        SingleOutputStreamOperator<String> resultStream = connected.process(new BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>() {

            /*BroadcastState<String, Tuple2<String, String>> broadcastState;*/

            /**
             * ������������������ �����е����ݣ�ÿ��һ��������һ�Σ�
             * @param element  �������������е�һ������
             * @param ctx  ������
             * @param out  �����
             * @throws Exception
             */
            @Override
            public void processElement(Tuple2<String, String> element, BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {

                // ͨ�� ReadOnlyContext ctx ȡ���Ĺ㲥״̬������һ�� ��ֻ�� �� �Ķ���
                ReadOnlyBroadcastState<String, Tuple2<String, String>> broadcastState = ctx.getBroadcastState(userInfoStateDesc);

                if (broadcastState != null) {
                    Tuple2<String, String> userInfo = broadcastState.get(element.f0);
                    out.collect(element.f0 + "," + element.f1 + "," + (userInfo == null ? null : userInfo.f0) + "," + (userInfo == null ? null : userInfo.f1));
                } else {
                    out.collect(element.f0 + "," + element.f1 + "," + null + "," + null);
                }

            }

            /**
             *
             * @param element  �㲥���е�һ������
             * @param ctx  ������
             * @param out �����
             * @throws Exception
             */
            @Override
            public void processBroadcastElement(Tuple3<String, String, String> element, BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>.Context ctx, Collector<String> out) throws Exception {

                // ���������У���ȡ�㲥״̬���󣨿ɶ���д��״̬����
                BroadcastState<String, Tuple2<String, String>> broadcastState = ctx.getBroadcastState(userInfoStateDesc);

                // Ȼ�󽫻�õ�����  �㲥�����ݣ� ��ֺ�װ��㲥״̬
                broadcastState.put(element.f0, Tuple2.of(element.f1, element.f2));
            }
        });


        resultStream.print();


        env.execute();

    }


}
