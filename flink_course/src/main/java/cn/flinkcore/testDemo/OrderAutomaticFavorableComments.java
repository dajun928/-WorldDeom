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
 * Desc 在电商领域会有这么一个场景，如果用户买了商品，在订单完成之后，一定时间之内没有做出评价，系统自动给与五星好评，
 * 今天我们使用Flink的定时器来实现这一功能。
 */
public class OrderAutomaticFavorableComments {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        env.enableCheckpointing(5000);

        //source
        DataStream<Tuple2<String, Long>> dataStream = env.addSource(new MySource());

        //经过interval毫秒用户未对订单做出评价，自动给与好评.为了演示方便，设置了5s的时间
        long interval = 5000L;
        //分组后使用自定义KeyedProcessFunction完成定时判断超时订单并自动好评
        dataStream.keyBy(0).process(new TimerProcessFuntion(interval));

        env.execute();
    }

    /**
     * 定时处理逻辑
     * 1.首先我们定义一个MapState类型的状态，key是订单号，value是订单完成时间
     * 2.在processElement处理数据的时候，把每个订单的信息存入状态中，这个时候不做任何处理，
     * 并且注册一个定时器(在订单完成时间+间隔时间(interval)时触发).
     * 3.注册的时器到达了订单完成时间+间隔时间(interval)时就会触发onTimer方法，我们主要在这个里面进行处理。
     * 我们调用外部的接口来判断用户是否做过评价，
     * 如果没做评价，调用接口给与五星好评，如果做过评价，则什么也不处理,最后记得把相应的订单从MapState删除
     */
    public static class TimerProcessFuntion extends KeyedProcessFunction<Tuple, Tuple2<String, Long>, Object> {
        //定义MapState类型的状态，key是订单号，value是订单完成时间
        private MapState<String, Long> mapState;
        //超过多长时间(interval,单位：毫秒) 没有评价，则自动五星好评
        private long interval = 0L;

        public TimerProcessFuntion(long interval) {
            this.interval = interval;
        }

        //创建MapState
        @Override
        public void open(Configuration parameters) {
            MapStateDescriptor<String, Long> mapStateDesc =
                    new MapStateDescriptor<>("mapStateDesc", String.class, Long.class);
            mapState = getRuntimeContext().getMapState(mapStateDesc);
        }

        //注册定时器
        @Override
        public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Object> out) throws Exception {
            mapState.put(value.f0, value.f1);
            ctx.timerService().registerProcessingTimeTimer(value.f1 + interval);
        }

        //定时器被触发时执行
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {
            Iterator iterator = mapState.iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Long> entry = (Map.Entry<String, Long>) iterator.next();
                String orderid = entry.getKey();
                boolean evaluated = isEvaluation(entry.getKey()); //调用方法判断订单是否已评价?
                mapState.remove(orderid);
                if (evaluated) {
                    System.out.println("订单(orderid: "+orderid+")在"+interval+"毫秒时间内已经评价，不做处理");
                }
                if (evaluated) {
                    //如果用户没有做评价，在调用相关的接口给与默认的五星评价
                    System.out.println("订单(orderid: "+orderid+")超过"+interval+"毫秒未评价，调用接口自动给与五星好评");
                }
            }
        }

        //自定义方法实现查询用户是否对该订单进行了评价，我们这里只是随便做了一个判断
        //在生产环境下，可以去查询相关的订单系统.
        private boolean isEvaluation(String key) {
            return key.hashCode() % 2 == 0;
        }
    }

    /**
     * 自定义source模拟生成一些订单数据.
     * 在这里，我们生了一个最简单的二元组Tuple2,包含订单id和订单完成时间两个字段.
     */
    public static class MySource implements SourceFunction<Tuple2<String, Long>> {
        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
            Random random = new Random();
            while (isRunning) {
                Thread.sleep(1000);
                //订单id
                String orderid = UUID.randomUUID().toString();
                //订单完成时间
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
