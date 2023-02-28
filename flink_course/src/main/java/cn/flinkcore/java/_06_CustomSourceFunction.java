package cn.flinkcore.java;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import javax.swing.plaf.TableHeaderUI;
import java.util.HashMap;
import java.util.Map;


/**
 *
 * @Author: deep as the sea
 * @Site: www.51doit.com
 * @QQ: 657270652
 * @Date: 2022/4/23
 * @Desc: �Զ���source����
 *
 * �Զ���source
 *    ����ʵ��   SourceFunction  ���� RichSourceFunction , �����߶��Ƿǲ��е�source����
 *    Ҳ��ʵ��   ParallelSourceFunction  ���� RichParallelSourceFunction , �����߶��ǿɲ��е�source����
 *
 * -- �� Rich�ģ���ӵ�� open() ,close() ,getRuntimeContext() ����
 * -- �� Parallel�ģ����ɶ�ʵ������ִ��
 **/
public class _06_CustomSourceFunction {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        DataStreamSource<EventLog> dataStreamSource = env.addSource(new MySourceFunction());
        DataStreamSource<HashMap<String, String>> dataStreamSource = env.addSource(new MyRedisSourceFunction());
        // DataStreamSource<EventLog> dataStreamSource = env.addSource(new MyParallelSourceFunction()).setParallelism(2);
        // DataStreamSource<EventLog> dataStreamSource = env.addSource(new MyRichSourceFunction());
        // DataStreamSource<EventLog> dataStreamSource = env.addSource(new MyRichParallelSourceFunction()).setParallelism(2);
        dataStreamSource.map(JSON::toJSONString).print();

        env.execute();
    }
}


class MySourceFunction implements SourceFunction<EventLog>{
    volatile boolean flag = true;

    @Override
    public void run(SourceContext<EventLog> ctx) throws Exception {

        EventLog eventLog = new EventLog();
        String[] events = {"appLaunch","pageLoad","adShow","adClick","itemShare","itemCollect","putBack","wakeUp","appClose"};
        HashMap<String, String> eventInfoMap = new HashMap<>();

        while(flag){

            eventLog.setGuid(RandomUtils.nextLong(1,1000));
            eventLog.setSessionId(RandomStringUtils.randomAlphabetic(12).toUpperCase());
            eventLog.setTimeStamp(System.currentTimeMillis());
            eventLog.setEventId(events[RandomUtils.nextInt(0,events.length)]);

            eventInfoMap.put(RandomStringUtils.randomAlphabetic(1),RandomStringUtils.randomAlphabetic(2));
            eventLog.setEventInfo(eventInfoMap);

            ctx.collect(eventLog);

            eventInfoMap.clear();

            Thread.sleep(RandomUtils.nextInt(200,1500));
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}

class MyParallelSourceFunction implements ParallelSourceFunction<EventLog>{

    @Override
    public void run(SourceContext<EventLog> ctx) throws Exception {

    }

    @Override
    public void cancel() {

    }
}


class MyRichSourceFunction extends RichSourceFunction<EventLog>{

    volatile boolean flag = true;
    /**
     * source�����ʼ��
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {

        RuntimeContext runtimeContext = getRuntimeContext();
        // ���Դ�����ʱ�������У�ȡ�������������� task ��task��
        String taskName = runtimeContext.getTaskName();
        // ���Դ�����ʱ�������У�ȡ�������������� subTask ��subTaskId
        int indexOfThisSubtask = runtimeContext.getIndexOfThisSubtask();


    }

    /**
     * source����������ݵĹ��̣����Ĺ����߼���
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<EventLog> ctx) throws Exception {
        EventLog eventLog = new EventLog();
        String[] events = {"appLaunch","pageLoad","adShow","adClick","itemShare","itemCollect","putBack","wakeUp","appClose"};
        HashMap<String, String> eventInfoMap = new HashMap<>();

        while(flag){

            eventLog.setGuid(RandomUtils.nextLong(1,1000));
            eventLog.setSessionId(RandomStringUtils.randomAlphabetic(12).toUpperCase());
            eventLog.setTimeStamp(System.currentTimeMillis());
            eventLog.setEventId(events[RandomUtils.nextInt(0,events.length)]);

            eventInfoMap.put(RandomStringUtils.randomAlphabetic(1),RandomStringUtils.randomAlphabetic(2));
            eventLog.setEventInfo(eventInfoMap);

            ctx.collect(eventLog);

            eventInfoMap.clear();

            Thread.sleep(RandomUtils.nextInt(500,1500));
        }
    }


    /**
     * jobȡ�����õķ���
     */
    @Override
    public void cancel() {
        flag = false;
    }

    /**
     * ����رյ��õķ���
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        System.out.println("������ر���.....");
    }
}

class MyRichParallelSourceFunction extends RichParallelSourceFunction<EventLog>{

    @Override
    public void run(SourceContext<EventLog> ctx) throws Exception {

    }

    @Override
    public void cancel() {

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        RuntimeContext runtimeContext = getRuntimeContext();
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}



/**
 *
 * ��redis�б�����й��Һʹ����Ĺ�ϵ
 * hset  areas AREA_US US
 * hset  areas AREA_CT TW,HK
 * hset  areas AREA_AR PK,KW,SA
 * hset  areas AREA_IN IN
 *./bin/kafka-console-consumer.sh --bootstrap-server hadoop01:9092,hadoop02:9092,hadoop03:9092 --topic allDataClean--from-beginning
 *
 * ������Ҫ����kv�Եģ���Ҫ����HashMap
 * ԭ�����ӣ�https://blog.csdn.net/weixin_39150719/article/details/102485124
 *
 *
 *   https://mvnrepository.com/artifact/redis.clients/jedis
 *             <dependency>
 *                 <groupId>redis.clients</groupId>
 *                 <artifactId>jedis</artifactId>
 *                 <version>2.9.3</version>
 *             </dependency>
 */


class MyRedisSourceFunction implements SourceFunction<HashMap<String,String>> {
    private Logger logger= LoggerFactory.getLogger(MyRedisSourceFunction.class);
    private boolean isRunning =true;
    private Jedis jedis=null;
    private final long SLEEP_MILLION=5000;
    public void run(SourceContext<HashMap<String, String>> ctx) throws Exception {
        this.jedis = new Jedis("localhost", 6379);
        HashMap<String, String> kVMap = new HashMap<String, String>();
        while(isRunning){
            try{
                kVMap.clear();
                Map<String, String> areas = jedis.hgetAll("areas");
                for(Map.Entry<String,String> entry:areas.entrySet()){
                    // key :���� value������
                    String key = entry.getKey();
                    String value = entry.getValue();
                    String[] splits = value.split(",");
                    System.out.println("key:"+key+"��--value��"+value);
                    for (String split:splits){
                        // key :����value������
                        kVMap.put(split, key);
                    }
                }
                if(kVMap.size()>0){
                    ctx.collect(kVMap);
                }else {
                    logger.warn("��redis�л�ȡ������Ϊ��");
                }
                Thread.sleep(SLEEP_MILLION);
            }catch (JedisConnectionException e){
                logger.warn("redis�����쳣����Ҫ��������",e.getCause());
                jedis = new Jedis("localhost", 6379);
            }catch (Exception e){
                logger.warn(" source ����Դ�쳣",e.getCause());
            }
        }
    }

    public void cancel() {
        isRunning=false;
        while(jedis!=null){
            jedis.close();
        }
    }
}


