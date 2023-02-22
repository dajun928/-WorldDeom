package cn.flink.java;

import com.alibaba.fastjson.JSON;
import lombok.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

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

        DataStreamSource<EventLog> dataStreamSource = env.addSource(new MySourceFunction());
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




