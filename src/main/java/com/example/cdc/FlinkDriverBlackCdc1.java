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


public class FlinkDriverBlackCdc1 {

    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setIdleStateRetentionTime(Time.minutes(5), Time.minutes(15));

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

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_test (id bigint,name string,proc_time AS PROCTIME()) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '127.0.0.1', " +
                " 'server-id' = '71229', " +
                "  'port' = '3306', " +
                "  'username' = 'root', " +
                "  'password' = 'ZYF123qwe', " +
                "  'database-name' = 'test', " +
                "  'table-name' = 'test', " +
                "  'server-time-zone' = 'Asia/Shanghai', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', " +
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");

//        Table tableQuery = tableEnv.sqlQuery("select id,mobile,is_black from ods_driver_black");

        Table tableQuery = tableEnv.sqlQuery("select id,count(*) from source_test group by id");
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
                }).print();
//                .addSink(new MysqlSink());
        env.execute("货主熟车司机拉黑场景实时表");
    }
}
