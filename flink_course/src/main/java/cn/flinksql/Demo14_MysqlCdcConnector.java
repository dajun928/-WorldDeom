package cn.flinksql;

import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Demo14_MysqlCdcConnector {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ¿ªÆôcheckpoint
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.explainSql("\n" +
                "create table if not EXISTS test.dim_product_info(\n" +
                "    id_ int ,\n" +
                "    product_id int,\n" +
                "    product_name string,\n" +
                "    PRIMARY KEY(id_) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'mysql-cdc',\n" +
                " 'hostname' = '192.168.43.131',\n" +
                " 'port' = '3306',\n" +
                " 'username' = 'root',\n" +
                " 'password' = '123456',\n" +
                " 'database-name' = 'test',\n" +
                " 'table-name' = 'product_info' \n" +
                ")");

        tableEnv.executeSql("select * from test.product_info").print();

        env.execute();

    }


}
