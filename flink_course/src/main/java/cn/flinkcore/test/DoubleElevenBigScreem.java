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
 * Desc ģ��˫ʮһ����ʵʱ������ʾ:
 * 1.ʵʱ�������������ֹ����ǰʱ��������ܶ�
 * 2.������������������top3
 * 3.ÿ���Ӹ���һ��ͳ�ƽ��
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
                        //�����СΪһ��Ĵ���,�ڶ���������ʾ�й�ʹ�õ�UTC+08:00ʱ����UTCʱ����8Сʱ
                        TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8))
                )
                .trigger(
                        ContinuousProcessingTimeTrigger.of(Time.seconds(1))//����һ��1s�Ĵ�����
                )
                .aggregate(new PriceAggregate(), new WindowResult());

        //��һ�¾ۺϽ��
        //result.print("�����ۺϽ��");

        //4.ʹ������ۺϵĽ��,ʵ��ҵ������:
        // * 1.ʵʱ�������������ֹ����ǰʱ��������ܶ�
        // * 2.������������������top3
        // * 3.ÿ���Ӹ���һ��ͳ�ƽ��
        result.keyBy("dateTime")
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))//ÿ���Ӹ���һ��ͳ�ƽ��
                .process(new WindowResultProcess());//��ProcessWindowFunction��ʵ�ָø���ҵ���߼�

        env.execute();
    }

    /**
     * �Զ���۸�ۺϺ���,��ʵ���Ƕ�price�ļ�sum����
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
     * �Զ���WindowFunction,ʵ������ռ����ڽ������
     */
    private static class WindowResult implements WindowFunction<Double, CategoryPojo, Tuple, TimeWindow> {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        public void apply(Tuple key, TimeWindow window, Iterable<Double> input, Collector<CategoryPojo> out) throws Exception {
            BigDecimal bg = new BigDecimal(input.iterator().next());
            double p = bg.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();//��������

            CategoryPojo categoryPojo = new CategoryPojo();
            categoryPojo.setCategory(((Tuple1<String>) key).f0);
            categoryPojo.setTotalPrice(p);
            categoryPojo.setDateTime(simpleDateFormat.format(new Date()));

            out.collect(categoryPojo);
        }
    }

    /**
     * ʵ��ProcessWindowFunction
     * ���������������Ľ��ͳ�ƣ�
     * �Ѹ���������ܼۼ�����������ȫվ����������
     * Ȼ������ͬʱʹ�����ȼ����м�����������۵�Top3��
     * ����ӡ���������ʵ�������ǿ��԰����������ݴ洢��hbase����redis�У��Թ�ǰ�˵�ʵʱҳ��չʾ��
     */
    private static class WindowResultProcess extends ProcessWindowFunction<CategoryPojo, Object, Tuple, TimeWindow> {
        @Override
        public void process(Tuple tuple, Context context, Iterable<CategoryPojo> elements, Collector<Object> out) throws Exception {
            String date = ((Tuple1<String>) tuple).f0;
            //���ȼ�����
            //ʵ�ʿ����г�ʹ��PriorityQueueʵ�ִ�С���������topK����
            //�����k��Ԫ�ص����⣺ʹ��С����
            //����Сk��Ԫ�ص����⣺ʹ�ô󶥶�
            //https://blog.csdn.net/hefenglian/article/details/81807527
            Queue<CategoryPojo> queue = new PriorityQueue<>(3,
                    (c1, c2) -> c1.getTotalPrice() >= c2.getTotalPrice() ? 1 : -1);//С����

            double price = 0D;
            Iterator<CategoryPojo> iterator = elements.iterator();
            int s = 0;
            while (iterator.hasNext()) {
                CategoryPojo categoryPojo = iterator.next();
                //ʹ�����ȼ����м����top3
                if (queue.size() < 3) {
                    queue.add(categoryPojo);
                } else {
                    //����topN��ʱ����ҪС����,Ҳ����Ҫȥ���Ѷ��Ƚ�С��Ԫ��
                    CategoryPojo tmp = queue.peek();//ȡ���Ѷ�Ԫ��
                    if (categoryPojo.getTotalPrice() > tmp.getTotalPrice()) {
                        queue.poll();//�Ƴ�
                        queue.add(categoryPojo);
                    }
                }
                price += categoryPojo.getTotalPrice();
            }

            //����TotalPrice����
            List<String> list = queue.stream().sorted((c1, c2) -> c1.getTotalPrice() <= c2.getTotalPrice() ? 1 : -1)//����
                    .map(c -> "(���ࣺ" + c.getCategory() + " ���۶" + c.getTotalPrice() + ")")
                    .collect(Collectors.toList());
            System.out.println("ʱ�� ��" + date);
            System.out.println("�ܼ� : " + new BigDecimal(price).setScale(2, RoundingMode.HALF_UP));
            System.out.println("Top3 : \n" + StringUtils.join(list, ",\n"));
            System.out.println("-------------");
        }
    }

    /**
     * ���ڴ洢�ۺϵĽ��
     */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CategoryPojo {
        private String category;//��������
        private double totalPrice;//�÷��������۶�
        private String dateTime;// ��ֹ����ǰʱ���ʱ��
    }

    /**
     * ģ������ĳһ�������µĶ���
     */
    public static class MySource implements SourceFunction<Tuple2<String, Double>> {
        private volatile boolean isRunning = true;
        private Random random = new Random();
        String category[] = {
                "Ůװ", "��װ",
                "ͼ��", "�ҵ�",
                "ϴ��", "��ױ",
                "�˶�", "��Ϸ",
                "����", "�Ҿ�",
                "����", "�칫"
        };

        @Override
        public void run(SourceContext<Tuple2<String, Double>> ctx) throws Exception {
            while (isRunning) {
                Thread.sleep(10);
                //�������һ������
                String c = category[(int) (Math.random() * (category.length - 1))];
                //�������һ���÷����µ�������ĳɽ�����
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
