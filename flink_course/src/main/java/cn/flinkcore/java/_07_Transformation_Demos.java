package cn.flinkcore.java;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.List;

/**
 * @Author: deep as the sea
 * @Site: www.51doit.com
 * @QQ: 657270652
 * @Date: 2022/4/23
 * @Desc: ����transformation���ӵ�api��ʾ
 **/
public class _07_Transformation_Demos {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // {"uid":1,"name":"zs","friends":[{"fid":2,"name":"aa"},{"fid":3,"name":"bb"}]}
        /*DataStreamSource<String> streamSource = env.fromElements(
                "{\"uid\":1,\"name\":\"zs\",\"friends\":[{\"fid\":2,\"name\":\"aa\"},{\"fid\":3,\"name\":\"bb\"}]}",
                "{\"uid\":2,\"name\":\"ls\",\"friends\":[{\"fid\":1,\"name\":\"cc\"},{\"fid\":2,\"name\":\"aa\"}]}",
                "{\"uid\":3,\"name\":\"ww\",\"friends\":[{\"fid\":2,\"name\":\"aa\"}]}",
                "{\"uid\":4,\"name\":\"zl\",\"friends\":[{\"fid\":3,\"name\":\"bb\"}]}",
                "{\"uid\":5,\"name\":\"tq\",\"friends\":[{\"fid\":2,\"name\":\"aa\"},{\"fid\":3,\"name\":\"bb\"}]}"
        );*/


        DataStreamSource<String> streamSource = env.readTextFile("flink_course/src/main/java/cn/flink/transformation_input/");


        /**
         * map���ӵ���ʾ
         */
        // ��ÿ��json���ݣ�ת��javabean����
        SingleOutputStreamOperator<UserInfo> beanStream = streamSource.map(json -> JSON.parseObject(json, UserInfo.class));
        /*beanStream.print();*/


        /**
         * filter���ӵ���ʾ
         *   ����˵����ѳ���3λ���û�����
         */
        SingleOutputStreamOperator<UserInfo> filtered = beanStream.filter(bean -> bean.getFriends().size() <= 3);
        /*filtered.print();*/


        /**
         * flatmap���ӵ���ʾ
         *  ��ÿ���û��ĺ�����Ϣ��ȫ����ȡ�����������û��������Ϣ��������ѹƽ������������
         *  {"uid":1,"name":"zs","gender":"male","friends":[{"fid":2,"name":"aa"},{"fid":3,"name":"bb"}]}
         *  =>
         *  {"uid":1,"name":"zs","gender":"male","fid":2,"fname":"aa"}
         *  {"uid":1,"name":"zs","gender":"male","fid":3,"fname":"bb"}
         */
        SingleOutputStreamOperator<UserFriendInfo> flatten = filtered.flatMap(new FlatMapFunction<UserInfo, UserFriendInfo>() {
            @Override
            public void flatMap(UserInfo value, Collector<UserFriendInfo> out) throws Exception {
                // ��friends�б���ȡ������һ��һ���ط���
                List<FriendInfo> friends = value.getFriends();
                /* friends.forEach(x->out.collect(new UserFriendInfo(value.getUid(), value.getName(), value.getGender(),x.getFid(),x.getName() )));*/
                for (FriendInfo x : friends) {
                    out.collect(new UserFriendInfo(value.getUid(), value.getName(), value.getGender(), x.getFid(), x.getName()));
                }
            }
        });
        /* flatten.print();*/


        /**
         * keyBy���ӵ���ʾ
         * ����һ���Ľ�������û��Ա����
         *
         * �����ۺ����ӣ�ֻ���� KeyedStream ���ϵ��ã���  sum���� �� min���� �� minBy���� �� max���ӡ�  maxBy���ӡ� reduce���ӵ���ʾ
         * ��ͳ�ƣ�
         *    ���Ա��û��ĺ�������
         *
         *    ���Ա��У��û������������ֵ
         *    ���Ա��У��û�����������Сֵ
         *
         *    ����Ա��У��������������Ǹ���
         *    ����Ա��У�����������С���Ǹ���
         *
         */
        // ���Ա��û��ĺ�������
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = flatten.map(bean -> Tuple2.of(bean.getGender(), 1)).returns(new TypeHint<Tuple2<String, Integer>>() {
                })
                .keyBy(tp -> tp.f0);

        keyedStream
                .sum(1);
        /*genderFriendCount.print();*/

        // ���Ա��У��û������������ֵ

        /**
         * max / maxBy  ���ǹ����ۺϣ�  ���Ӳ�����յ�����������ȫ��������������ֻ��״̬�м�¼��һ�εľۺ�ֵ��Ȼ�������ݵ����ʱ�򣬻�����߼�ȥ���� ״̬�м�¼�ľۺ�ֵ�����������״̬����
         * max / maxBy  ���� ����״̬���߼���  maxֻ����Ҫ�����ֵ���ֶΣ�  �� maxBy ������������ݣ�
         * ͬ��min��minByҲ���
         */
        SingleOutputStreamOperator<Tuple4<Integer, String, String, Integer>> tuple4Stream = beanStream.map(bean -> Tuple4.of(bean.getUid(), bean.getName(), bean.getGender(), bean.getFriends().size())).returns(new TypeHint<Tuple4<Integer, String, String, Integer>>() {
        });


        tuple4Stream
                .keyBy(tp -> tp.f2)
                /*.max(3); */              // ���Ա��У��û������������ֵ
                .maxBy(3);  // ����Ա��У��������������Ǹ���

        /*genderUserFriendsMaxCount.print();*/


        /**
         * reduce ���� ʹ����ʾ
         * ���� ����Ա��У��������������Ǹ��ˣ��������ǰ�������˵ĺ���������ͬ��������Ľ���У�Ҳ��Ҫ��uid/name����Ϣ���³ɺ���һ�����ݵ�ֵ
         *
         */
        SingleOutputStreamOperator<Tuple4<Integer, String, String, Integer>> reduceResult = tuple4Stream.keyBy(tp -> tp.f2)
                .reduce(new ReduceFunction<Tuple4<Integer, String, String, Integer>>() {
                    /**
                     *
                     * @param value1  �Ǵ�ǰ�ľۺϽ��
                     * @param value2  �Ǳ��ε�������
                     * @return ���º�ľۺϽ��
                     * @throws Exception
                     */
                    @Override
                    public Tuple4<Integer, String, String, Integer> reduce(Tuple4<Integer, String, String, Integer> value1, Tuple4<Integer, String, String, Integer> value2) throws Exception {

                        if (value1 == null || value2.f3 >= value1.f3) {
                            return value2;
                        } else {
                            return value1;
                        }
                    }
                });
        /*reduceResult.print();*/


        /**
         * ��reduce����ʵ��sum���ӵĹ���
         * ��  �������  4Ԫ������  1,ua,male,2  ������Ա�ĺ������ܺ�
         * TODO
         */
        SingleOutputStreamOperator<Tuple4<Integer, String, String, Integer>> reduceSum = tuple4Stream.keyBy(tp -> tp.f2)
                .reduce(new ReduceFunction<Tuple4<Integer, String, String, Integer>>() {
                    @Override
                    public Tuple4<Integer, String, String, Integer> reduce(Tuple4<Integer, String, String, Integer> value1, Tuple4<Integer, String, String, Integer> value2) throws Exception {
                        value2.setField(value2.f3 + (value1 == null ? 0 : value1.f3), 3);
                        return value2;
                    }
                });
        reduceSum.print();


        env.execute();

    }
}

@Data
class UserInfo implements Serializable {
    private int uid;
    private String name;
    private String gender;
    private List<FriendInfo> friends;
}

@Data
class FriendInfo implements Serializable {
    private int fid;
    private String name;
}

@Data
@AllArgsConstructor
class UserFriendInfo implements Serializable {
    private int uid;
    private String name;
    private String gender;
    private int fid;
    private String fname;

}