package cn.flinksql;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;

public class _01_WordCount {

    public static void main(String[] args) throws Exception {

        // ������spark�� sparkContext���õ� rdd
        // StreamExecutionEnvironment,�õ����� stream
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple2<String,Integer>> s1 = source.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String,Integer>> out) throws Exception {
                String[] arr = value.split(" ");
                for (String w : arr) {
                    out.collect(Tuple2.of(w,1));
                }
            }
        });


        /**
         * ��1������������ӳ��ɱ�
         */
        // �� ����� tableApi�еı����
        Table table = tEnv.fromDataStream(s1, "word,cnt");  // tableApi
        // table.select().groupBy()

        // �� �����sql�е���ͼ����
        // ��sql�����Ļ�����Ԫ�����У���ӵ����һ�ű����� t1
        tEnv.createTemporaryView("t1",s1);   // ����ע��ɱ�����Ĭ�����ɵ�Schema ==>  f0:String ��f1:Int
        //tEnv.executeSql("desc t1").print();  // ��ѯ t1�ı�ṹ��Schema��
        /*
           +------+--------+-------+-----+--------+-----------+
           | name |   type |  null | key | extras | watermark |
           +------+--------+-------+-----+--------+-----------+
           |   f0 | STRING |  true |     |        |           |
           |   f1 |    INT | false |     |        |           |
           +------+--------+-------+-----+--------+-----------+
        */

        tEnv.createTemporaryView("t2",s1,"word,cnt");  // ����ע��ɱ����ַ�����ʽ���ֶ�ָ��Schema�е��ֶ���
        //tEnv.executeSql("desc t2").print();  // ��ѯ t2�ı�ṹ��Schema��
        /*
            +------+--------+------+-----+--------+-----------+
            | name |   type | null | key | extras | watermark |
            +------+--------+------+-----+--------+-----------+
            | word | STRING | true |     |        |           |
            |  cnt |    INT | true |     |        |           |
            +------+--------+------+-----+--------+-----------+
        */
        tEnv.createTemporaryView("t3",s1,$("word"),$("cnt"));  // ����ע��ɱ��ñ��ʽ��ʽ���ֶ�ָ��Schema�е��ֶ���
        //tEnv.executeSql("desc t3").print();  // ��ѯ t2�ı�ṹ��Schema��


        /**
         * �°汾api����Schema�������ṹʱ�������ֶΣ�����ͨ��������Ϣ�⹹�������ֶΣ���Ĭ���Զ������ڱ�ṹ�У�f0,f1
         * ������������ײ�⹹�������ֶ���Ϣ��ͬ�Ķ���, �ǵö�������µ��߼��ֶΣ��߼��ֶΣ��ǿ����ñ��ʽ���������ֶ������ɣ�
         */
        tEnv.createTemporaryView("t4",s1, Schema.newBuilder()
                .column("f0", DataTypes.STRING())  // columnָ�������ֶ������൱��ɶҲû������������ָ�� �ֶε�����
                .column("f1", DataTypes.INT())  //
                .columnByExpression("word",$("f0").upperCase())  // ����һ���µ��߼��ֶΣ������ֽ�word���������ڵײ�f0�ֶε�������   create table t_x( f0 string,f1 int , word as upper(f0) ) ;
                .columnByExpression("cnt",$("f1").cast(DataTypes.BIGINT()))  // ����һ���µ��߼��ֶΣ������ֽ�cnt���������ڵײ�f1�ֶΣ�������������ת��
                .columnByExpression("cnt2","cast(f1 as bigint)")  // ����һ���µ��߼��ֶΣ����ֽ�cnt2�������ڵײ������ֶ�f1,���ұ��ʽ��sql�ַ������ʽ
                .columnByMetadata("rt",DataTypes.TIMESTAMP_LTZ(3),"rowtime")    // �ӵײ�����Դ����������connector���г�ȡĳ��Ԫ���ݣ�������һ������߼��ֶ�
                .build());
        tEnv.executeSql("desc t4").print();
        /**
         * +------+--------+------+-----+-----------------------+-----------+
         * | name |   type | null | key |                extras | watermark |
         * +------+--------+------+-----+-----------------------+-----------+
         * |   f0 | STRING | true |     |                       |           |
         * |   f1 |    INT | true |     |                       |           |
         * | word | STRING | true |     |          AS upper(f0) |           |
         * |  cnt | BIGINT | true |     |   AS cast(f1, BIGINT) |           |
         * | cnt2 | BIGINT | true |     | AS cast(f1 as bigint) |           |
         * +------+--------+------+-----+-----------------------+-----------+
         */
        tEnv.executeSql("select * from t4").print();




        /**
         * ��2������дsql�߼�
         */
        tEnv.executeSql("select * from t2")/*.print()*/;
        // ����Ľ����ֻ����Insertģʽ�����ݣ�����AppendOnly��append����

        tEnv.executeSql("select word,sum(cnt)  as cnt from t2 group by word")/*.print()*/;
        /**
         * ����Ľ�����ڵײ���һ�� updated table��changelog��)
         * +----+--------------------------------+-------------+
         * | op |                           word |         cnt |
         * +----+--------------------------------+-------------+
         * | +I |                              a |           1 |
         * | -U |                              a |           1 |
         * | +U |                              a |           2 |
         * | -U |                              a |           2 |
         * | +U |                              a |           3 |
         * | +I |                              b |           1 |
         */


        // �������õ���stream�����ӣ�����Ҫ����evn.execute()��
        // �����ü�
        env.execute();

    }
}
