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
 * @Desc: 自定义source算子
 *
 * 自定义source
 *    可以实现   SourceFunction  或者 RichSourceFunction , 这两者都是非并行的source算子
 *    也可实现   ParallelSourceFunction  或者 RichParallelSourceFunction , 这两者都是可并行的source算子
 *
 * -- 带 Rich的，都拥有 open() ,close() ,getRuntimeContext() 方法
 * -- 带 Parallel的，都可多实例并行执行
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
     * source组件初始化
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {

        RuntimeContext runtimeContext = getRuntimeContext();
        // 可以从运行时上下文中，取到本算子所属的 task 的task名
        String taskName = runtimeContext.getTaskName();
        // 可以从运行时上下文中，取到本算子所属的 subTask 的subTaskId
        int indexOfThisSubtask = runtimeContext.getIndexOfThisSubtask();


    }

    /**
     * source组件生成数据的过程（核心工作逻辑）
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
     * job取消调用的方法
     */
    @Override
    public void cancel() {
        flag = false;
    }

    /**
     * 组件关闭调用的方法
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        System.out.println("组件被关闭了.....");
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
 * 在redis中保存的有国家和大区的关系
 * hset  areas AREA_US US
 * hset  areas AREA_CT TW,HK
 * hset  areas AREA_AR PK,KW,SA
 * hset  areas AREA_IN IN
 *./bin/kafka-console-consumer.sh --bootstrap-server hadoop01:9092,hadoop02:9092,hadoop03:9092 --topic allDataClean--from-beginning
 *
 * 我们需要返回kv对的，就要考虑HashMap
 * 原文链接：https://blog.csdn.net/weixin_39150719/article/details/102485124
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
                    // key :大区 value：国家
                    String key = entry.getKey();
                    String value = entry.getValue();
                    String[] splits = value.split(",");
                    System.out.println("key:"+key+"，--value："+value);
                    for (String split:splits){
                        // key :国家value：大区
                        kVMap.put(split, key);
                    }
                }
                if(kVMap.size()>0){
                    ctx.collect(kVMap);
                }else {
                    logger.warn("从redis中获取的数据为空");
                }
                Thread.sleep(SLEEP_MILLION);
            }catch (JedisConnectionException e){
                logger.warn("redis连接异常，需要重新连接",e.getCause());
                jedis = new Jedis("localhost", 6379);
            }catch (Exception e){
                logger.warn(" source 数据源异常",e.getCause());
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


