package cn.flink.java;


import com.alibaba.fastjson.JSON;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.Optional;

/**
 *
 * @Author: deep as the sea
 * @Site: www.51doit.com
 * @QQ: 657270652
 * @Date: 2022/4/26
 * @Desc:
 *     ��������д��redis������RedisSink����
 *
 **/
public class _12_RedisSinkOperator_Demo1 {


    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port",8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);


        // ����checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");

        // �����һ��������
        DataStreamSource<EventLog> streamSource = env.addSource(new MySourceFunction());

        // eventLog���ݲ���redis��������ʲô�ṹ���洢��
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder().setHost("localhost").build();

        RedisSink<EventLog> redisSink = new RedisSink<>(config, new StringInsertMapper());
        streamSource.print();
        streamSource.addSink(redisSink);

        env.execute();

    }


    static class StringInsertMapper implements RedisMapper<EventLog>{

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.SET);
        }

        /**
         *  ���ѡ�����û���ڲ�key��redis���ݽṹ����˷������صľ��Ǵ� key
         *  ���ѡ��������ڲ�key��redis���ݽṹ��hset������˷������ص���hset�ڲ���Сkey����������Description�д����ֵ��Ϊ��key
         * @param data
         * @return
         */
        @Override
        public String getKeyFromData(EventLog data) {
            return data.getGuid()+"-"+data.getSessionId()+"-"+data.getTimeStamp();   // �������string���ݵĴ�key
        }

        @Override
        public String getValueFromData(EventLog data) {
            return JSON.toJSONString(data);   // �������string���ݵ�value
        }
    }


    /**
     * HASH�ṹ���ݲ���
     */
    static class HsetInsertMapper implements RedisMapper<EventLog>{
        // ���Ը��ݾ������ݣ� ѡ�����key������hash���ֽṹ�����ж���key����key��
        @Override
        public Optional<String> getAdditionalKey(EventLog data) {
            return RedisMapper.super.getAdditionalKey(data);
        }

        // ���Ը��ݾ������ݣ����ò�ͬ��TTL��time to live�����ݵĴ��ʱ����
        @Override
        public Optional<Integer> getAdditionalTTL(EventLog data) {
            return RedisMapper.super.getAdditionalTTL(data);
        }

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET,"event-logs");
        }

        /**
         *  ���ѡ�����û���ڲ�key��redis���ݽṹ����˷������صľ��Ǵ� key
         *  ���ѡ��������ڲ�key��redis���ݽṹ��hset������˷������ص���hset�ڲ���Сkey����������Description�д����ֵ��Ϊ��key
         * @param data
         * @return
         */
        @Override
        public String getKeyFromData(EventLog data) {
            return data.getGuid()+"-"+data.getSessionId()+"-"+data.getTimeStamp();  // �������hset�е�field��Сkey��
        }

        @Override
        public String getValueFromData(EventLog data) {
            return data.getEventId();   // �������hset�е�value
        }


    }






}
