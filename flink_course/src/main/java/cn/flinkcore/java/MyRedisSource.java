package cn.flinkcore.java;


import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.HashMap;
import java.util.Map;


/**
 * ��redis�б�����й��Һʹ����Ĺ�ϵ
 * hset  areas AREA_US US
 * hset  areas AREA_CT TW,HK
 * hset  areas AREA_AR PK,KW,SA
 * hset  areas AREA_IN IN
 * ./bin/kafka-console-consumer.sh --bootstrap-server hadoop01:9092,hadoop02:9092,hadoop03:9092 --topic allDataClean--from-beginning
 * <p>
 * ������Ҫ����kv�Եģ���Ҫ����HashMap
 */
public class MyRedisSource implements SourceFunction<HashMap<String, String>> {
    private Logger logger = LoggerFactory.getLogger(MyRedisSource.class);
    private boolean isRunning = true;
    private Jedis jedis = null;
    private final long SLEEP_MILLION = 5000;

    public void run(SourceContext<HashMap<String, String>> ctx) throws Exception {
        this.jedis = new Jedis("hadoop01", 6379);
        HashMap<String, String> kVMap = new HashMap<String, String>();
        while (isRunning) {
            try {
                kVMap.clear();
                Map<String, String> areas = jedis.hgetAll("areas");
                for (Map.Entry<String, String> entry : areas.entrySet()) {
                    // key :���� value������
                    String key = entry.getKey();
                    String value = entry.getValue();
                    String[] splits = value.split(",");
                    System.out.println("key:" + key + "��--value��" + value);
                    for (String split : splits) {
                        // key :����value������
                        kVMap.put(split, key);
                    }
                }
                if (kVMap.size() > 0) {
                    ctx.collect(kVMap);
                } else {
                    logger.warn("��redis�л�ȡ������Ϊ��");
                }
                Thread.sleep(SLEEP_MILLION);
            } catch (JedisConnectionException e) {
                logger.warn("redis�����쳣����Ҫ��������", e.getCause());
                jedis = new Jedis("hadoop01", 6379);
            } catch (Exception e) {
                logger.warn(" source ����Դ�쳣", e.getCause());
            }
        }
    }

    public void cancel() {
        isRunning = false;
        while (jedis != null) {
            jedis.close();
        }
    }
}
