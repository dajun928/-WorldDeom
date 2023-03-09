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
//        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE flink_learn.dim_product_info(\n" +
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
                " 'database-name' = 'flink_learn',\n" +
                " 'table-name' = 'product_info' \n" +
                " )");

        tableEnv.executeSql("select * from flink_learn.product_info").print();

        env.execute();

    }


}

//

//        SLF4J: Defaulting to no-operation (NOP) logger implementation
//        SLF4J: See http://www.slf4j.org/codes.html#noProviders for further details.
//        SLF4J: Class path contains SLF4J bindings targeting slf4j-api versions prior to 1.8.
//        SLF4J: Ignoring binding found at [jar:file:/C:/developer/maven/org/slf4j/slf4j-log4j12/1.7.30/slf4j-log4j12-1.7.30.jar!/org/slf4j/impl/StaticLoggerBinder.class]
//        SLF4J: Ignoring binding found at [jar:file:/C:/developer/maven/ch/qos/logback/logback-classic/1.2.9/logback-classic-1.2.9.jar!/org/slf4j/impl/StaticLoggerBinder.class]
//        SLF4J: See http://www.slf4j.org/codes.html#ignoredBindings for an explanation.
//        Exception in thread "main" org.apache.flink.table.api.ValidationException: Could not execute CreateTable in path `default_catalog`.`flink_learn`.`dim_product_info`
//        at org.apache.flink.table.catalog.CatalogManager.execute(CatalogManager.java:845)
//        at org.apache.flink.table.catalog.CatalogManager.createTable(CatalogManager.java:659)
//        at org.apache.flink.table.api.internal.TableEnvironmentImpl.executeInternal(TableEnvironmentImpl.java:881)
//        at org.apache.flink.table.api.internal.TableEnvironmentImpl.executeSql(TableEnvironmentImpl.java:742)
//        at cn.flinksql.Demo14_MysqlCdcConnector.main(Demo14_MysqlCdcConnector.java:21)
//        Caused by: org.apache.flink.table.catalog.exceptions.DatabaseNotExistException: Database flink_learn does not exist in Catalog default_catalog.
//        at org.apache.flink.table.catalog.GenericInMemoryCatalog.createTable(GenericInMemoryCatalog.java:215)
//        at org.apache.flink.table.catalog.CatalogManager.lambda$createTable$10(CatalogManager.java:661)
//        at org.apache.flink.table.catalog.CatalogManager.execute(CatalogManager.java:841)
//        ... 4 more