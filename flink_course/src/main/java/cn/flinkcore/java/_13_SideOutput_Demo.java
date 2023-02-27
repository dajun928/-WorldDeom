package cn.flinkcore.java;


import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">���׽���</a>
 * @QQ: 657270652
 * @Date: 2022/4/26
 * @Desc: ������� ����ʾ����process���ӣ�
 **/
public class _13_SideOutput_Demo {


    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);


        // ����checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");

        // �����һ��������
        DataStreamSource<EventLog> streamSource = env.addSource(new MySourceFunction());


        // ���� ����Ϊ�¼��������з���
        //     appLaunch �¼� ���ֵ�һ����
        //     putBack �¼����ֵ�һ����
        //     �����¼�����������
        SingleOutputStreamOperator<EventLog> processed = streamSource.process(new ProcessFunction<EventLog, EventLog>() {
            /**
             *
             * @param eventLog  ��������
             * @param ctx �����ģ������ṩ�������������
             * @param out ��������ռ���
             * @throws Exception
             */
            @Override
            public void processElement(EventLog eventLog, ProcessFunction<EventLog, EventLog>.Context ctx, Collector<EventLog> out) throws Exception {
                String eventId = eventLog.getEventId();

                if ("appLaunch".equals(eventId)) {

                    ctx.output(new OutputTag<EventLog>("launch", TypeInformation.of(EventLog.class)), eventLog);

                } else if ("putBack".equals(eventId)) {

                    ctx.output(new OutputTag<String>("back",TypeInformation.of(String.class)), JSON.toJSONString(eventLog));
                }

                out.collect(eventLog);

            }
        });

        // ��ȡ  launch ��������
        DataStream<EventLog> launchStream = processed.getSideOutput(new OutputTag<EventLog>("launch", TypeInformation.of(EventLog.class)));

        // ��ȡback ��������
        DataStream<String> backStream = processed.getSideOutput(new OutputTag<String>("back",TypeInformation.of(String.class)));

        launchStream.print("launch");

        backStream.print("back");


        env.execute();

    }


}
