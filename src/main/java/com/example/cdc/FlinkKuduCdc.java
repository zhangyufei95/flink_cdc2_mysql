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
import org.slf4j.LoggerFactory;
import org.apache.log4j.Logger;

public class FlinkKuduCdc {
    private static final Logger logger = Logger.getLogger(FlinkKuduCdc.class);
    public static void main(String[] args) throws Exception {



        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_wx_external_platform_mapping (id bigint,type int,platform_u_id string) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '1215', " +
                "  'port' = '3306', " +
                "  'username' = 'flink_cdc_ad', " +
                "  'password' = 'jklsek52@=9H', " +
                "  'database-name' = 'ht_user', " +
                "  'table-name' = 'wx_external_platform_mapping', " +
                "  'server-time-zone' = 'Asia/Shanghai', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', "+
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_wx_tag_relation (tag_id bigint,foreign_id bigint,status int,type int,create_time timestamp,update_time timestamp) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '1216', " +
                "  'port' = '3306', " +
                "  'username' = 'flink_cdc_ad', " +
                "  'password' = 'jklsek52@=9H', " +
                "  'database-name' = 'ht_user', " +
                "  'table-name' = 'wx_tag_relation', " +
                "  'server-time-zone' = 'Asia/Shanghai', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', "+
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_wx_tag (id bigint,car_type int,car_length decimal(10,2),status int,type int) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '1217', " +
                "  'port' = '3306', " +
                "  'username' = 'flink_cdc_ad', " +
                "  'password' = 'jklsek52@=9H', " +
                "  'database-name' = 'ht_user', " +
                "  'table-name' = 'wx_tag', " +
                "  'server-time-zone' = 'Asia/Shanghai', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', "+
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");

        Table tableQuery = tableEnv.sqlQuery("select\n" +
                "        wepm1.platform_u_id\n" +
                "        ,wt.car_type\n" +
                "        ,wt.car_length\n" +
                "        ,date_format(wtr.create_time,'yyyy-MM-dd HH:mm:ss')\n" +
                "    FROM\n" +
                "        (\n" +
                "            SELECT\n" +
                "                    *\n" +
                "                FROM\n" +
                "                    source_wx_external_platform_mapping\n" +
                "                where type = 1\n" +
                "        ) wepm1\n" +
                "            inner JOIN (\n" +
                "                SELECT\n" +
                "                        *\n" +
                "                    FROM\n" +
                "                        source_wx_tag_relation\n" +
                "                    WHERE\n" +
                "                        (date_format(create_time,'yyyy-MM-dd') = date_format(current_timestamp,'yyyy-MM-dd') or date_format(update_time,'yyyy-MM-dd') = date_format(current_timestamp,'yyyy-MM-dd'))\n" +
                "                        AND status = 1\n" +
                "                        AND type = 3\n" +
                "            ) wtr\n" +
                "                ON wepm1.id = wtr.foreign_id\n" +
                "            LEFT JOIN (\n" +
                "                SELECT\n" +
                "                        *\n" +
                "                    FROM\n" +
                "                        source_wx_tag\n" +
                "                    where\n" +
                "                   \n" +
                "                         status = 1\n" +
                "                        AND type = 3\n" +
                "            ) wt\n" +
                "                ON wtr.tag_id = wt.id\n" +
                "                AND wtr.type = wt.type\n" +
                "                where platform_u_id>0");
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
        }).addSink(new FlinkKuduCdc.KuduSink());
        env.execute("今日实时司机-车型-车长记录");
    }

    public static class KuduSink extends RichSinkFunction<Row> {
        //master地址
        final String masteraddr = "172.27.16.41:7051,172.27.16.43:7051,172.27.16.49:7051";

        private KuduTable table = null;
        private KuduClient client = null;
        private  KuduSession session = null;
        @Override
        public void open(Configuration parameters) throws Exception {
            //创建kudu连接
            client = new KuduClient.KuduClientBuilder(masteraddr).defaultSocketReadTimeoutMs(6000).build();
            //打开表
            table = client.openTable("stage.stage_ht_user_wx_tag_di");
            // 创建写session,kudu必须通过session写入
            session = client.newSession();
        }

        @Override
        public void close() throws Exception {
            if (session != null) {
                session.close();
            }
            if (client != null) {
                client.close();
            }
        }
        /**
         * 数据输出时执行，每一个数据输出时，都会执行此方法
         *
         * @param value
         * @param context
         * @throws Exception
         */
        @Override
        public void invoke(Row value, Context context) throws Exception {
            try {
                // 采取Flush方式 手动刷新
                session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
                session.setMutationBufferSpace(3000);
                Upsert upsert = table.newUpsert();
                PartialRow row = upsert.getRow();
                // 设置字段内容
                row.addString("puid", value.getField(0).toString());
                row.addString("car_type", value.getField(1).toString());
                row.addString("car_length", value.getField(2).toString());
                row.addString("create_time", value.getField(3).toString());
                session.apply(upsert);
                session.flush();
                logger.info(value.toString());
                System.out.println(value);
            } catch (Exception e) {
                e.printStackTrace();
                logger.debug(e.getMessage(),e);
                System.out.println("Wrong!");
            }
            System.out.println("Done Successfully!");
        }
    }
}

