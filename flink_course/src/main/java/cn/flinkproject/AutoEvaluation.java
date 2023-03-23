package cn.flinkproject;


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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

/**
 * @author zhangjun ��ӭ��ע�ҵĹ��ں�[�����ݼ�����Ӧ��ʵս],��ȡ���ྫ��ʵս����
 *
 * �ڵ�����վ������Ʒ���������֮������û�24Сʱ֮��û���ۣ�ϵͳ�Զ�������
 * ����ͨ��flink�Ķ�ʱ�����򵥵�ʵ���������
 * https://blog.csdn.net/zhangjun5965/article/details/106796423/?utm_medium=distribute.pc_relevant.none-task-blog-2~default~baidujs_utm_term~default-3--blog-127168064.pc_relevant_3mothn_strategy_recovery&spm=1001.2101.3001.4242.3&utm_relevant_index=6
 */
public class AutoEvaluation{

    private static final Logger LOG = LoggerFactory.getLogger(AutoEvaluation.class);

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        DataStream<Tuple2<String,Long>> dataStream = env.addSource(new MySource());
        //����interval�����û�δ�Զ����������ۣ��Զ��������.
        //����Ϊ����ʾ���㣬������5s��ʱ��
        long interval = 5000l;
        dataStream.keyBy(0).process(new TimerProcessFuntion(interval));
        env.execute();
    }

    public static class TimerProcessFuntion
            extends KeyedProcessFunction<Tuple,Tuple2<String,Long>,Object>{

        private MapState<String,Long> mapState;
        //�����೤ʱ��(interval,��λ������) û�����ۣ����Զ����Ǻ���
        private long interval = 0l;

        public TimerProcessFuntion(long interval){
            this.interval = interval;
        }

        @Override
        public void open(Configuration parameters){
            MapStateDescriptor<String,Long> mapStateDesc = new MapStateDescriptor<>(
                    "mapStateDesc",
                    String.class, Long.class);
            mapState = getRuntimeContext().getMapState(mapStateDesc);
        }

        @Override
        public void onTimer(
                long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception{
            Iterator iterator = mapState.iterator();
            while (iterator.hasNext()){
                Map.Entry<String,Long> entry = (Map.Entry<String,Long>) iterator.next();

                String orderid = entry.getKey();
                boolean f = isEvaluation(entry.getKey());
                mapState.remove(orderid);
                if (f){
                    LOG.info("����(orderid: {}) ��  {} ����ʱ�����Ѿ����ۣ���������", orderid, interval);
                    System.out.println(orderid);
                    System.out.println(interval);
                }
                if (f){
                    //����û�û�������ۣ��ڵ�����صĽӿڸ���Ĭ�ϵ���������
                    LOG.info("����(orderid: {}) ����  {} ����δ���ۣ����ýӿڸ��������Զ�����", orderid, interval);
                    System.out.println(orderid);
                    System.out.println(interval);
                }
            }
        }

        /**
         * �û��Ƿ�Ըö������������ۣ������������£�����ȥ��ѯ��صĶ���ϵͳ.
         * ��������ֻ���������һ���ж�
         *
         * @param key
         * @return
         */
        private boolean isEvaluation(String key){
            return key.hashCode() % 2 == 0;
        }

        @Override
        public void processElement(
                Tuple2<String,Long> value, Context ctx, Collector<Object> out) throws Exception{
            mapState.put(value.f0, value.f1);
            ctx.timerService().registerProcessingTimeTimer(value.f1 + interval);
        }
    }

    public static class MySource implements SourceFunction<Tuple2<String,Long>>{
        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Tuple2<String,Long>> ctx) throws Exception{
            while (isRunning){
                Thread.sleep(1000);
                //����id
                String orderid = UUID.randomUUID().toString();
                //�������ʱ��
                long orderFinishTime = System.currentTimeMillis();
                ctx.collect(Tuple2.of(orderid, orderFinishTime));
            }
        }

        @Override
        public void cancel(){
            isRunning = false;
        }
    }
}
