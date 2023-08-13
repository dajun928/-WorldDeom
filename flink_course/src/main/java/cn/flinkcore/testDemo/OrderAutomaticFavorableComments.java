package cn.flinkcore.testDemo;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * Author Lansonli
 * Desc �ڵ������������ôһ������������û�������Ʒ���ڶ������֮��һ��ʱ��֮��û���������ۣ�ϵͳ�Զ��������Ǻ�����
 * ��������ʹ��Flink�Ķ�ʱ����ʵ����һ���ܡ�
 */
public class OrderAutomaticFavorableComments {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        env.enableCheckpointing(5000);

        //source
        DataStream<Tuple2<String, Long>> dataStream = env.addSource(new MySource());

        //����interval�����û�δ�Զ����������ۣ��Զ��������.Ϊ����ʾ���㣬������5s��ʱ��
        long interval = 5000L;
        //�����ʹ���Զ���KeyedProcessFunction��ɶ�ʱ�жϳ�ʱ�������Զ�����
        dataStream.keyBy(0).process(new TimerProcessFuntion(interval));

        env.execute();
    }

    /**
     * ��ʱ�����߼�
     * 1.�������Ƕ���һ��MapState���͵�״̬��key�Ƕ����ţ�value�Ƕ������ʱ��
     * 2.��processElement�������ݵ�ʱ�򣬰�ÿ����������Ϣ����״̬�У����ʱ�����κδ���
     * ����ע��һ����ʱ��(�ڶ������ʱ��+���ʱ��(interval)ʱ����).
     * 3.ע���ʱ�������˶������ʱ��+���ʱ��(interval)ʱ�ͻᴥ��onTimer������������Ҫ�����������д���
     * ���ǵ����ⲿ�Ľӿ����ж��û��Ƿ��������ۣ�
     * ���û�����ۣ����ýӿڸ������Ǻ���������������ۣ���ʲôҲ������,���ǵð���Ӧ�Ķ�����MapStateɾ��
     */
    public static class TimerProcessFuntion extends KeyedProcessFunction<Tuple, Tuple2<String, Long>, Object> {
        //����MapState���͵�״̬��key�Ƕ����ţ�value�Ƕ������ʱ��
        private MapState<String, Long> mapState;
        //�����೤ʱ��(interval,��λ������) û�����ۣ����Զ����Ǻ���
        private long interval = 0L;

        public TimerProcessFuntion(long interval) {
            this.interval = interval;
        }

        //����MapState
        @Override
        public void open(Configuration parameters) {
            MapStateDescriptor<String, Long> mapStateDesc =
                    new MapStateDescriptor<>("mapStateDesc", String.class, Long.class);
            mapState = getRuntimeContext().getMapState(mapStateDesc);
        }

        //ע�ᶨʱ��
        @Override
        public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Object> out) throws Exception {
            mapState.put(value.f0, value.f1);
            ctx.timerService().registerProcessingTimeTimer(value.f1 + interval);
        }

        //��ʱ��������ʱִ��
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {
            Iterator iterator = mapState.iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Long> entry = (Map.Entry<String, Long>) iterator.next();
                String orderid = entry.getKey();
                boolean evaluated = isEvaluation(entry.getKey()); //���÷����ж϶����Ƿ�������?
                mapState.remove(orderid);
                if (evaluated) {
                    System.out.println("����(orderid: "+orderid+")��"+interval+"����ʱ�����Ѿ����ۣ���������");
                }
                if (evaluated) {
                    //����û�û�������ۣ��ڵ�����صĽӿڸ���Ĭ�ϵ���������
                    System.out.println("����(orderid: "+orderid+")����"+interval+"����δ���ۣ����ýӿ��Զ��������Ǻ���");
                }
            }
        }

        //�Զ��巽��ʵ�ֲ�ѯ�û��Ƿ�Ըö������������ۣ���������ֻ���������һ���ж�
        //�����������£�����ȥ��ѯ��صĶ���ϵͳ.
        private boolean isEvaluation(String key) {
            return key.hashCode() % 2 == 0;
        }
    }

    /**
     * �Զ���sourceģ������һЩ��������.
     * �������������һ����򵥵Ķ�Ԫ��Tuple2,��������id�Ͷ������ʱ�������ֶ�.
     */
    public static class MySource implements SourceFunction<Tuple2<String, Long>> {
        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
            Random random = new Random();
            while (isRunning) {
                Thread.sleep(1000);
                //����id
                String orderid = UUID.randomUUID().toString();
                //�������ʱ��
                long orderFinishTime = System.currentTimeMillis();
                ctx.collect(Tuple2.of(orderid, orderFinishTime));
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
