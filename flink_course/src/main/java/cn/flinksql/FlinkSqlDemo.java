package cn.flinksql;

import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/3/14
 **/
public class FlinkSqlDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // stream -> table
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);


        SingleOutputStreamOperator<EventBean> stream = source.map(s -> JSON.parseObject(s, EventBean.class));


         // [Table Sql] 方式
        // 将dataStream注册成临时视图
        tableEnv.createTemporaryView("t_event",stream);

        // 执行sql
        String sql = "select guid,sessionId,eventId,count(1) as num from t_event group by guid,sessionId,eventId ";
        tableEnv.sqlQuery(sql).execute().print();



        // 2.[Table Api] 方式
        // 从dataStream 创建 Table 对象
//        Table table = tableEnv.fromDataStream(stream);
//
//        Table select = table.groupBy($("guid"), $("sessionId"), $("eventId"))
//                .aggregate($("eventId").count().as("num"))
//                .select($("guid"), $("sessionId"), $("eventId"), $("num"));
//
//        select.execute().print();


        env.execute();

    }
}
