package cn.flinkcore.java;

import com.alibaba.fastjson.JSON;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Author: deep as the sea
 * @Site: www.51doit.com
 * @QQ: 657270652
 * @Date: 2022/4/26
 * @Desc: ��������д�� mysql������ JdbcSink����
 *
 *
 * CREATE TABLE `t_eventlog` (
 *   `guid` bigint(20) NOT NULL,
 *   `sessionId` varchar(255) DEFAULT NULL,
 *   `eventId` varchar(255) DEFAULT NULL,
 *   `ts` bigint(20) DEFAULT NULL,
 *   `eventInfo` varchar(255) DEFAULT NULL,
 *   PRIMARY KEY (`guid`)
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
 **/
public class _11_JdbcSinkOperatorSR {


    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);


        // ����checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");

        // �����һ��������
        DataStreamSource<EventLog> streamSource = env.addSource(new MySourceFunction());


        /**
         *  һ�� ����֤ EOS����ķ�ʽ
         */
        SinkFunction<EventLog> jdbcSink = JdbcSink.sink(
                "insert into t_eventlog values (?,?,?,?,?) on duplicate key update sessionId=?,eventId=?,ts=?,eventInfo=? ",
                new JdbcStatementBuilder<EventLog>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, EventLog eventLog) throws SQLException {
                        preparedStatement.setLong(1, eventLog.getGuid());
                        preparedStatement.setString(2, eventLog.getSessionId());
                        preparedStatement.setString(3, eventLog.getEventId());
                        preparedStatement.setLong(4, eventLog.getTimeStamp());
                        preparedStatement.setString(5, JSON.toJSONString(eventLog.getEventInfo()));

                        preparedStatement.setString(6, eventLog.getSessionId());
                        preparedStatement.setString(7, eventLog.getEventId());
                        preparedStatement.setLong(8, eventLog.getTimeStamp());
                        preparedStatement.setString(9, JSON.toJSONString(eventLog.getEventInfo()));
                    }
                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(3)
                        .withBatchSize(1)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUsername("root")
                        .withPassword("123456")
                        .withUrl("jdbc:mysql://192.168.43.131:3306/flink_learn")
                        .build()
        );

        // �������
        streamSource.addSink(jdbcSink);


        /**
         * ���������ṩ EOS ���屣֤�� sink todo �е����� ����о�
         */
//        SinkFunction<EventLog> exactlyOnceSink = JdbcSink.exactlyOnceSink(
//                "insert into t_eventlog values (?,?,?,?,?) on duplicate key update sessionId=?,eventId=?,ts=?,eventInfo=? ",
//                new JdbcStatementBuilder<EventLog>() {
//                    @Override
//                    public void accept(PreparedStatement preparedStatement, EventLog eventLog) throws SQLException {
//                        preparedStatement.setLong(1, eventLog.getGuid());
//                        preparedStatement.setString(2, eventLog.getSessionId());
//                        preparedStatement.setString(3, eventLog.getEventId());
//                        preparedStatement.setLong(4, eventLog.getTimeStamp());
//                        preparedStatement.setString(5, JSON.toJSONString(eventLog.getEventInfo()));
//
//                        preparedStatement.setString(6, eventLog.getSessionId());
//                        preparedStatement.setString(7, eventLog.getEventId());
//                        preparedStatement.setLong(8, eventLog.getTimeStamp());
//                        preparedStatement.setString(9, JSON.toJSONString(eventLog.getEventInfo()));
//                    }
//                },
//                JdbcExecutionOptions.builder()
//                        .withMaxRetries(3)
//                        .withBatchSize(1)
//                        .build(),
//                JdbcExactlyOnceOptions.builder()
//                        // mysql��֧��ͬһ�������ϴ��ڲ��еĶ�����񣬱���Ѹò�������Ϊtrue
//                        .withTransactionPerConnection(true)
//                        .build(),
//                new SerializableSupplier<XADataSource>() {
//                    @Override
//                    public XADataSource get() {
//                        // XADataSource����jdbc���ӣ���������֧�ֲַ�ʽ���������
//                        // �������Ĺ��췽������ͬ�����ݿ⹹�췽����ͬ
//                        MysqlXADataSource xaDataSource = new MysqlXADataSource();
//                        xaDataSource.setUser("root");
//                        xaDataSource.setUrl("jdbc:mysql://192.168.43.131:3306/flink_learn");
//                        xaDataSource.setPassword("123456");
//                        return xaDataSource;
//                    }
//                }
//        );

//         �������
//        streamSource.addSink(exactlyOnceSink);



        env.execute();
    }
}
