package cn.flinkcore.test;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Author Lansonli
 * Desc 模拟双十一电商实时大屏显示:
 * 1.实时计算出当天零点截止到当前时间的销售总额
 * 2.计算出各个分类的销售top3
 * 3.每秒钟更新一次统计结果
 */
public class DoubleElevenBigScreem {
    public static void main(String[] args) throws Exception {
        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.source
        DataStream<Tuple2<String, Double>> dataStream = env.addSource(new MySource());

        //3.transformation
        DataStream<CategoryPojo> result = dataStream
                .keyBy(0)
                .window(
                        //定义大小为一天的窗口,第二个参数表示中国使用的UTC+08:00时区比UTC时间早8小时
                        TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8))
                )
                .trigger(
                        ContinuousProcessingTimeTrigger.of(Time.seconds(1))//定义一个1s的触发器
                )
                .aggregate(new PriceAggregate(), new WindowResult());

        //看一下聚合结果
        //result.print("初步聚合结果");

        //4.使用上面聚合的结果,实现业务需求:
        // * 1.实时计算出当天零点截止到当前时间的销售总额
        // * 2.计算出各个分类的销售top3
        // * 3.每秒钟更新一次统计结果
        result.keyBy("dateTime")
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))//每秒钟更新一次统计结果
                .process(new WindowResultProcess());//在ProcessWindowFunction中实现该复杂业务逻辑

        env.execute();
    }

    /**
     * 自定义价格聚合函数,其实就是对price的简单sum操作
     */
    private static class PriceAggregate implements AggregateFunction<Tuple2<String, Double>, Double, Double> {
        @Override
        public Double createAccumulator() {
            return 0D;
        }

        @Override
        public Double add(Tuple2<String, Double> value, Double accumulator) {
            return accumulator + value.f1;
        }

        @Override
        public Double getResult(Double accumulator) {
            return accumulator;
        }

        @Override
        public Double merge(Double a, Double b) {
            return a + b;
        }
    }

    /**
     * 自定义WindowFunction,实现如何收集窗口结果数据
     */
    private static class WindowResult implements WindowFunction<Double, CategoryPojo, Tuple, TimeWindow> {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        public void apply(Tuple key, TimeWindow window, Iterable<Double> input, Collector<CategoryPojo> out) throws Exception {
            BigDecimal bg = new BigDecimal(input.iterator().next());
            double p = bg.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();//四舍五入

            CategoryPojo categoryPojo = new CategoryPojo();
            categoryPojo.setCategory(((Tuple1<String>) key).f0);
            categoryPojo.setTotalPrice(p);
            categoryPojo.setDateTime(simpleDateFormat.format(new Date()));

            out.collect(categoryPojo);
        }
    }

    /**
     * 实现ProcessWindowFunction
     * 在这里我们做最后的结果统计，
     * 把各个分类的总价加起来，就是全站的总销量金额，
     * 然后我们同时使用优先级队列计算出分类销售的Top3，
     * 最后打印出结果，在实际中我们可以把这个结果数据存储到hbase或者redis中，以供前端的实时页面展示。
     */
    private static class WindowResultProcess extends ProcessWindowFunction<CategoryPojo, Object, Tuple, TimeWindow> {
        @Override
        public void process(Tuple tuple, Context context, Iterable<CategoryPojo> elements, Collector<Object> out) throws Exception {
            String date = ((Tuple1<String>) tuple).f0;
            //优先级队列
            //实际开发中常使用PriorityQueue实现大小顶堆来解决topK问题
            //求最大k个元素的问题：使用小顶堆
            //求最小k个元素的问题：使用大顶堆
            //https://blog.csdn.net/hefenglian/article/details/81807527
            Queue<CategoryPojo> queue = new PriorityQueue<>(3,
                    (c1, c2) -> c1.getTotalPrice() >= c2.getTotalPrice() ? 1 : -1);//小顶堆

            double price = 0D;
            Iterator<CategoryPojo> iterator = elements.iterator();
            int s = 0;
            while (iterator.hasNext()) {
                CategoryPojo categoryPojo = iterator.next();
                //使用优先级队列计算出top3
                if (queue.size() < 3) {
                    queue.add(categoryPojo);
                } else {
                    //计算topN的时候需要小顶堆,也就是要去掉堆顶比较小的元素
                    CategoryPojo tmp = queue.peek();//取出堆顶元素
                    if (categoryPojo.getTotalPrice() > tmp.getTotalPrice()) {
                        queue.poll();//移除
                        queue.add(categoryPojo);
                    }
                }
                price += categoryPojo.getTotalPrice();
            }

            //按照TotalPrice逆序
            List<String> list = queue.stream().sorted((c1, c2) -> c1.getTotalPrice() <= c2.getTotalPrice() ? 1 : -1)//逆序
                    .map(c -> "(分类：" + c.getCategory() + " 销售额：" + c.getTotalPrice() + ")")
                    .collect(Collectors.toList());
            System.out.println("时间 ：" + date);
            System.out.println("总价 : " + new BigDecimal(price).setScale(2, RoundingMode.HALF_UP));
            System.out.println("Top3 : \n" + StringUtils.join(list, ",\n"));
            System.out.println("-------------");
        }
    }

    /**
     * 用于存储聚合的结果
     */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CategoryPojo {
        private String category;//分类名称
        private double totalPrice;//该分类总销售额
        private String dateTime;// 截止到当前时间的时间
    }

    /**
     * 模拟生成某一个分类下的订单
     */
    public static class MySource implements SourceFunction<Tuple2<String, Double>> {
        private volatile boolean isRunning = true;
        private Random random = new Random();
        String category[] = {
                "女装", "男装",
                "图书", "家电",
                "洗护", "美妆",
                "运动", "游戏",
                "户外", "家具",
                "乐器", "办公"
        };

        @Override
        public void run(SourceContext<Tuple2<String, Double>> ctx) throws Exception {
            while (isRunning) {
                Thread.sleep(10);
                //随机生成一个分类
                String c = category[(int) (Math.random() * (category.length - 1))];
                //随机生成一个该分类下的随机金额的成交订单
                double price = random.nextDouble() * 100;
                ctx.collect(Tuple2.of(c, price));
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
