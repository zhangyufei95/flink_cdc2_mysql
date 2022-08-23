package com.example.cdc;

import org.apache.flink.api.common.time.Time;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.io.UnsupportedEncodingException;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import java.util.Date;
import java.text.SimpleDateFormat;

import java.sql.Connection;
import java.sql.PreparedStatement;

import static java.sql.JDBCType.NULL;


public class FlinkDriverBlackCdc {

    static DefaultMQProducer producer = new DefaultMQProducer(UUID.randomUUID().toString().replaceAll("-", ""));
    static{
         //消息发送失败重试次数
         producer.setSendMsgTimeout(60000);
         producer.setRetryTimesWhenSendFailed(3);
         //消息没有存储成功是否发送到另外一个broker
         producer.setRetryAnotherBrokerWhenNotStoreOK(true);
         //指定NameServer 地址
         producer.setNamesrvAddr("172.27.16.17:9876;172.27.16.14:9876");
         //初始化Producer, 整个应用生命周期内只需要初始化1次
        try {
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setIdleStateRetentionTime(Time.hours(24), Time.hours(25));

//        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  ods_driver_black (id int,mobile string,is_black int) with(" +
//                " 'connector' = 'mysql-cdc', " +
//                " 'hostname' = '172.27.16.38', " +
//                " 'server-id' = '45759', " +
//                "  'port' = '3306', " +
//                "  'username' = 'root', " +
//                "  'password' = 'Zhicang@2017', " +
//                "  'database-name' = 'algo_nlp_dev', " +
//                "  'table-name' = 'driver_black', " +
//                "  'server-time-zone' = 'Asia/Shanghai', " +
//                "  'debezium.snapshot.locking.mode' = 'none', " +
//                "  'scan.incremental.snapshot.enabled' = 'false', " +
//                "  'debezium.snapshot.mode' = 'initial' " +
//                ")");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_wechat_thirdpart_follow_user (id bigint,black int,suite_id string,qw_user_id bigint,owner_id bigint,status int,deleted int,user_id string,remark_mobiles string,oper_user_id string,corp_id string,external_contact_id bigint,from_type int) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '71229', " +
                "  'port' = '3306', " +
                "  'username' = 'flink_cdc_ad', " +
                "  'password' = 'jklsek52@=9H', " +
                "  'database-name' = 'ht_user', " +
                "  'table-name' = 'wechat_thirdpart_follow_user', " +
                "  'server-time-zone' = 'Asia/Shanghai', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', "+
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");

//        Table tableQuery = tableEnv.sqlQuery("select id,mobile,is_black from ods_driver_black");

        Table tableQuery = tableEnv.sqlQuery("select qw_user_id,remark_mobiles,black from source_wechat_thirdpart_follow_user where from_type = 2 and deleted = 0");
        tableEnv.toRetractStream(tableQuery, Row.class)
                .filter(
                new FilterFunction<Tuple2<Boolean, Row>>() {
                    @Override
                    public boolean filter(Tuple2<Boolean, Row> booleanRowTuple2) throws Exception {
                        return booleanRowTuple2.f1.getKind().toString().equals("UPDATE_AFTER");
                    }
                }
        )
                .map(new MapFunction<Tuple2<Boolean, Row>, Row>() {
            @Override
            public Row map(Tuple2<Boolean, Row> booleanRowTuple2) throws Exception {
                return booleanRowTuple2.f1;
            }
        }).addSink(new MysqlSink());
        env.execute("货主熟车司机拉黑场景实时表");
    }

    public static void rocketMQProducer(String value) throws MQClientException, InterruptedException, MQBrokerException, RemotingException {
//        for (int i = 0; i < 100; i++) {
            //创建一条消息对象，指定其主题、标签和消息内容
            Message msg = new Message(
                    "HT_CALLER"/* 消息主题名*/,
                    "super_dispatch_customer"/*消息标签*/,
                    ("" + value).getBytes(StandardCharsets.UTF_8) // getBytes(RemotingHelper.DEFAULT_CHARSET)/*消息内容*/
            );
//            msg.setWaitStoreMsgOK(true);
            //发送消息并返回结果
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);
//        }
        //一旦生产者实例不再被使用则将其关闭，包括清理资源，关闭网络连接等
//        producer.shutdown();
    }
    public static class MysqlSink extends RichSinkFunction<Row> {
        /**
         * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
         *
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        @Override
        public void close() throws Exception {
            super.close();
            //关闭连接和释放资源
        }

        /**
         * 每条数据的插入都要调用一次 invoke() 方法
         *
         * @param value
         * @param context
         * @throws Exception
         */
        @Override
        public void invoke(Row value, Context context) throws Exception {
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String now_date= df.format(new Date());
            System.out.println(value.toString());
            try {
                String v0 = "";
                String v1 = "";
                int v2 = 0;
                if(null!=value.getField(0)){
                    v0=value.getField(0).toString();
                }
                if(null!=value.getField(1)){
                    v1=value.getField(1).toString();
                }
                if(null!=value.getField(2)){
                    v2=Integer.parseInt(value.getField(2).toString());
                }

                JSONObject jsonObject = new JSONObject(new LinkedHashMap());
                jsonObject.put("consumerId", v0);
                jsonObject.put("mobile", v1);
                if (v2==1) {
                    jsonObject.put("action", 2);
                } else if (v2==0) {
                    jsonObject.put("action", 3);
                }
                System.out.println(jsonObject.toJSONString() );
                rocketMQProducer(jsonObject.toJSONString());
            } catch (Exception e) {
                System.out.println(e.getMessage());
            } finally {
//                System.out.println("["+now_date+"] :成功地插入了1行数据, "+value.toString());
            }
        }
    }
}


//public class FlinkDriverBlackCdc {
//
//    public static void main(String[] args) throws Exception {
//        rocketMQProducer();
//        rocketMQConsumer();
//    }
//
//    public static void rocketMQConsumer() {
//        try {
//            System.out.println("rocketMQConsumer  开始------");
//            // 消费目标
//            // 声明一个消费者consumer，需要传入一个组
//            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("biz_group");
//            // 设置集群的NameServer地址，多个地址之间以分号分隔
//
//            consumer.setNamesrvAddr("10.0.10.12:9876");
//            // 设置consumer的消费策略
//            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
//            // 集群模式消费，广播消费不会重试
//            consumer.setMessageModel(MessageModel.CLUSTERING);
//            // 设置最大重试次数，默认是16次
//            consumer.setMaxReconsumeTimes(5);
//            // 设置consumer所订阅的Topic和Tag，*代表全部的Tag
//            consumer.subscribe("HT_CALLER", "customer_blacklist_msg");
//            // Listener，主要进行消息的逻辑处理,监听topic，如果有消息就会立即去消费
//            consumer.registerMessageListener(new MessageListenerConcurrently() {
//                @Override
//                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
//                    // 获取第一条消息，进行处理
//                    try {
//                        if (msgs != null && msgs.size() > 0) {
//                            MessageExt messageExt = msgs.get(0);
//                            String msgBody = new String(messageExt.getBody(), "utf-8");
//                            System.out.println(" 接收消息整体为：" + msgBody);
//                        }
//                    } catch (Exception e) {
//                        System.out.println("消息消费失败，请尝试重试！！！");
//                        e.printStackTrace();
//                        // 尝试重新消费，直接第三次如果还不成功就放弃消费，进行消息消费失败补偿操作
//                        if (msgs.get(0).getReconsumeTimes() == 3) {
//                            System.out.println("消息记录日志：" + msgs);
//                            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
//                        } else {
//                            // 重试状态码，重试机制可配置
//                            // System.out.println("消息消费失败，尝试重试！！！");
//                            System.out.println("消息消费失败，请尝试重试！！！");
//                            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
//                        }
//                    }
//                    System.out.println("消息消费成功！！！");
//                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
//                }
//            });
//            // 调用start()方法启动consumer
//            consumer.start();
//            System.out.println("消费者启动成功。。。");
//            System.out.println("rocketMQConsumer 结束------");
//        } catch (Exception e) {
//            e.printStackTrace();
//            System.out.println("消息消费操作失败--" + e.getMessage());
//        }
//    }
//
//    public static void rocketMQProducer() throws MQClientException, InterruptedException, MQBrokerException, RemotingException {
//        DefaultMQProducer producer = new DefaultMQProducer("biz_group",false);
//        //消息发送失败重试次数
//        producer.setSendMsgTimeout(60000);
//        producer.setRetryTimesWhenSendFailed(3);
//        //消息没有存储成功是否发送到另外一个broker
//        producer.setRetryAnotherBrokerWhenNotStoreOK(true);
//        //指定NameServer 地址
//        producer.setNamesrvAddr("10.0.10.12:9876");
//        //初始化Producer, 整个应用生命周期内只需要初始化- -次
//        producer.start();
//        for (int i = 0; i < 100; i++) {
//            //创建一条消息对象，指定其主题、标签和消息内容
//            Message msg = new Message(
//                    "HT_CALLER"/* 消息主题名*/,
//                    "customer_blacklist_msg"/*消息标签*/,
//                    ("Hello Java demo RocketMQ " + i).getBytes(StandardCharsets.UTF_8) // getBytes(RemotingHelper.DEFAULT_CHARSET)/*消息内容*/
//            );
////            msg.setWaitStoreMsgOK(true);
//            //发送消息并返回结果
//            SendResult sendResult = producer.send(msg);
//            System.out.printf("%s%n", sendResult);
//        }
//        //一旦生产者实例不再被使用则将其关闭，包括清理资源，关闭网络连接等
//        producer.shutdown();
//    }
//}