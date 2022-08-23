package com.example.cdc;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kudu.client.*;

public class FlinkKudu2Cdc {
    public static void main(String[] args) throws Exception {
        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.exec.sink.not-null-enforcer","drop");
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_wx_external_platform_mapping (" +
                "id bigint," +
                "external_user_id string," +
                "type int," +
                "platform_u_id string," +
                "update_time timestamp) " +
                "with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                "  'port' = '3306', " +
                "  'username' = 'flink_cdc_ad', " +
                "  'password' = 'jklsek52@=9H', " +
                "  'database-name' = 'ht_user', " +
                "  'table-name' = 'wx_external_platform_mapping', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', "+
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_wechat_external_contact (" +
                "id bigint," +
                "external_user_id string," +
                "create_time timestamp" +
                ") " +
                "with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                "  'port' = '3306', " +
                "  'username' = 'flink_cdc_ad', " +
                "  'password' = 'jklsek52@=9H', " +
                "  'database-name' = 'ht_user', " +
                "  'table-name' = 'wechat_external_contact', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', "+
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_wx_message (" +
                "id bigint," +
                "direction int," +
                "platform_msg_type int," +
                "platform_u_id string," +
                "profile_platform_u_id string," +
                "session_type int," +
                "text_content string," +
                "session_id string," +
                "create_time timestamp," +
                "dispatch_id bigint," +
                "client_id string," +
                "push_ack int," +
                "send_date timestamp(3)" +
                "WATERMARK FOR send_date AS send_date"  +
                ") with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                "  'port' = '3306', " +
                "  'username' = 'flink_cdc_ad', " +
                "  'password' = 'jklsek52@=9H', " +
                "  'database-name' = 'ht_user', " +
                "  'table-name' = 'wx_message', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', "+
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_wx_msg_task (" +
                "id bigint," +
                "demand_id bigint," +
                "wl_id string," +
                "task_id bigint," +
                "type int," +
                "create_time timestamp," +
                "template_id bigint," +
                "client_id string" +
                ") with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                "  'port' = '3306', " +
                "  'username' = 'flink_cdc_ad', " +
                "  'password' = 'jklsek52@=9H', " +
                "  'database-name' = 'ht_user', " +
                "  'table-name' = 'wx_msg_task', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', "+
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_dispatch_relation (" +
                "id bigint," +
                "dispatch_customer_id bigint," +
                "dispatch_customer_type int," +
                "create_time timestamp" +
//                "WATERMARK FOR create_time AS create_time" +
                ") with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                "  'port' = '3306', " +
                "  'username' = 'flink_cdc_ad', " +
                "  'password' = 'jklsek52@=9H', " +
                "  'database-name' = 'ht_user', " +
                "  'table-name' = 'dispatch_relation', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', "+
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");



        Table tableQuery = tableEnv.sqlQuery("select\n" +
                "wm.platform_u_id as p_u_id,\n" +
                "wm.profile_platform_u_id as user_id,\n" +
                "if(wm.direction = 2,cast(wm.create_time as string),'') as userid_first_send_time,\n" +
                "if(wm.direction = 1,cast(wm.create_time as string),'') as puid_first_send_time,\n" +
                "if(wm.direction = 2,cast(wm.create_time as string),'') as userid_send_time,\n" +
                "if(wm.direction = 1,cast(wm.create_time as string),'') as puid_send_time\n" +
//                "if(wmt.type = 10,cast(wmt.create_time as string),'') as person_push_time,\n" +
//                "if(wmt.type = 3, cast(wmt.create_time as string),'') as im_push_time\n" +
                "from\n" +
                "(select * from source_wx_message \n" +
                "where date_format(create_time,'yyyy-MM-dd') = date_format(current_timestamp,'yyyy-MM-dd') and session_type=1 and locate('可以开始聊天',text_content)=0 and platform_msg_type<>1011) wm\n" +
                "");
//                "left join\n" +
//                "(select client_id,wl_id,task_id,template_id,demand_id,type,create_time from source_wx_msg_task where date_format(create_time,'yyyy-MM-dd') = date_format(current_timestamp,'yyyy-MM-dd'))wmt \n" +
//                "on wm.client_id=wmt.client_id\n" +
//                "where (wm.platform_u_id is not null and wm.profile_platform_u_id is not null) and (locate('t_suc',wm.client_id)<>0\n" +
//                "or (wmt.type=3 and wm.direction=2)\n" +
//                "or (wmt.type=4 and wm.direction=2)\n" +
//                "or (wmt.type=10 and wm.direction=2)\n" +
//                "or ((wmt.type=1 or wmt.type is null) and wm.direction=2) or (wm.direction=1))");
        //Table tableQuery = tableEnv.sqlQuery("select left_table.id,left_table.demand_id,right_table.push_driver_count,right_table.high_intent_driver_count,right_table.un_reply_driver_count,current_timestamp from (select id,demand_id,push_driver_count,high_intent_driver_count,un_reply_driver_count from source_demand_heat_stat) left_table left join (select a.demand_id,b.wl_id_num as push_driver_count,c.high_intention_user-1 as high_intent_driver_count,c.reply_user-1 as un_reply_driver_count,current_timestamp from (select demand_id from source_contracted_route where demand_id > 0 and order_shipment in (0,2) and date_format(create_time,'yyyy-MM-dd') = date_format(current_timestamp,'yyyy-MM-dd') group by demand_id) a join (select demand_id,count(wl_id) as wl_id_num from source_wx_msg_task where demand_id > 0 and date_format(create_time,'yyyy-MM-dd') = date_format(current_timestamp,'yyyy-MM-dd') group by demand_id) b on a.demand_id = b.demand_id join (select demand_id,count(distinct if(intention>0,customer_id,0)) as high_intention_user,count(distinct if(intention=1,customer_id,0)) as reply_user from source_driver_intention where demand_id > 0 and date_format(create_time,'yyyy-MM-dd') = date_format(current_timestamp,'yyyy-MM-dd') group by demand_id) c on a.demand_id = c.demand_id) right_table on left_table.demand_id = right_table.demand_id");

        tableEnv.toRetractStream(tableQuery, Row.class).filter(
                new FilterFunction<Tuple2<Boolean, Row>>() {
                    @Override
                    public boolean filter(Tuple2<Boolean, Row> booleanRowTuple2) throws Exception {
                        return booleanRowTuple2.f0;
                    }
                }
        ).map(new MapFunction<Tuple2<Boolean, Row>, Row>() {
            @Override
            public Row map(Tuple2<Boolean, Row> booleanRowTuple2) throws Exception {
                return booleanRowTuple2.f1;
            }
        }).print();
//                addSink(new FlinkKuduCdc.KuduSink());
        env.execute();
    }

    public static class KuduSink extends RichSinkFunction<Row> {
        //master地址
        final String masteraddr = "172.27.16.41:7051,172.27.16.43:7051,172.27.16.49:7051";

        /**
         * 数据输出时执行，每一个数据输出时，都会执行此方法
         *
         * @param value
         * @param context
         * @throws Exception
         */
        @Override
        public void invoke(Row value, Context context) throws Exception {
            System.out.println(value.getField(0).toString() +';'+ value.getField(1).toString()+';'+ value.getField(2).toString()+';'+ value.getField(3).toString()+';'+ value.getField(4).toString()+';'+ value.getField(5).toString()+';'+ value.getField(6).toString()+';'+ value.getField(7).toString());

            try {
                //创建kudu的数据库链接
                KuduClient client = new KuduClient.KuduClientBuilder(masteraddr).defaultSocketReadTimeoutMs(6000).build();
                //打开表
                KuduTable table = client.openTable("stage.stage_driver_portrait_profile_df");
                KuduScanner.KuduScannerBuilder builder = client.newScannerBuilder(table);

                // 创建写session,kudu必须通过session写入
                KuduSession session = client.newSession();
                // 采取Flush方式 手动刷新
                session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
                session.setMutationBufferSpace(3000);
                Upsert upsert = table.newUpsert();
                System.out.println(value.getField(0).toString() +';'+ value.getField(1).toString()+';'+ value.getField(2).toString()+';'+ value.getField(3).toString()+';'+ value.getField(4).toString()+';'+ value.getField(5).toString()+';'+ value.getField(6).toString()+';'+ value.getField(7).toString());
                // 设置字段内容
                upsert.getRow().addString("p_u_id", value.getField(0).toString());
                upsert.getRow().addString("user_id", value.getField(1).toString());
                upsert.getRow().addString("userid_first_send_time", value.getField(2).toString());
                upsert.getRow().addString("puid_first_send_time", value.getField(3).toString());
                upsert.getRow().addString("userid_send_time", value.getField(4).toString());
                upsert.getRow().addString("puid_send_time", value.getField(5).toString());
                upsert.getRow().addString("person_push_time", value.getField(6).toString());
                upsert.getRow().addString("im_push_time", value.getField(7).toString());
                session.flush();
                session.apply(upsert);
                session.close();
//                client.close();
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Wrong!");
            }
            System.out.println("Done Successfully!");
        }
    }
}

