package com.example.cdc;

import com.alibaba.fastjson.JSONObject;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kudu.client.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kudu.client.*;
import org.slf4j.LoggerFactory;
import org.apache.log4j.Logger;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class FlinkHotTagCdc2 {
    private static final Logger logger = Logger.getLogger(FlinkHotTagCdc2.class);
    public static void main(String[] args) throws Exception {
        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_wx_external_platform_mapping (id bigint,type int,platform_u_id string) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '49915', " +
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

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_wx_tag_relation (tag_id bigint,user_tag_id bigint,create_user_id bigint,foreign_id bigint,status int,type int,create_time timestamp,update_time timestamp) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '49916', " +
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

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_user_dispatch_city_group (city_id bigint,city_cluster string) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '49917', " +
                "  'port' = '3306', " +
                "  'username' = 'flink_cdc_ad', " +
                "  'password' = 'jklsek52@=9H', " +
                "  'database-name' = 'ht_user', " +
                "  'table-name' = 'user_dispatch_city_group', " +
                "  'server-time-zone' = 'Asia/Shanghai', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', "+
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_base_geo_info (id bigint,parent_id bigint,name string,type int) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '49918', " +
                "  'port' = '3306', " +
                "  'username' = 'flink_cdc_ad', " +
                "  'password' = 'jklsek52@=9H', " +
                "  'database-name' = 'ht_travel', " +
                "  'table-name' = 'base_geo_info', " +
                "  'server-time-zone' = 'Asia/Shanghai', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', "+
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_wx_tag (id bigint,name string,c_id bigint,status int,type int) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '49919', " +
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

        Table tableQuery = tableEnv.sqlQuery("select * from (select puid,if(max(create_user_id)<776, 'algo', 'other') as tag_type,max(create_time) as create_time,max(release_date) as release_date,collect(distinct empty_city_id) as empty_city_id,collect(distinct empty_city_cluster) as empty_city_cluster,collect(distinct intention_city_id) as intention_city_id,collect(distinct intention_city_cluster) as intention_city_cluster,collect(distinct ban_push_time) as ban_push_time from (SELECT\n" +
                "m.platform_u_id as puid,\n" +
                "date_format(r.create_time,'yyyy-MM-dd HH:mm:ss') as create_time,\n" +
                "r.create_user_id,\n" +
                "if(r.user_tag_id=10000,from_unixtime(cast(cast(t.name as bigint)/1000 as int),'yyyy-MM-dd'),' ') as release_date,\n" +
                "if(r.user_tag_id=10001,if(r.status=0,' ',cast(t.c_id as string)),' ') as empty_city_id,\n" +
                "if(r.user_tag_id=10001,if(r.status=0,' ',ccl.city_cluster),' ') as empty_city_cluster,\n" +
                "if(r.user_tag_id=10002,if(r.status=0,' ',cast(t.c_id as string)),' ') as intention_city_id,\n" +
                "if(r.user_tag_id=10002,if(r.status=0,' ',ccl.city_cluster),' ') as intention_city_cluster,\n" +
                "if(r.user_tag_id=10003,if(r.status=0,' ',from_unixtime(cast(cast(t.name as bigint)/1000 as int),'yyyy-MM-dd')),' ') as ban_push_time\n" +
                "FROM\n" +
                "(select * from source_wx_tag_relation) r \n" +
                "join (select * from source_wx_tag) t on r.tag_id=t.id and t.status=1\n" +
                "join (select * from source_wx_external_platform_mapping) m on m.id=r.foreign_id\n" +
                "left join \n" +
                "(SELECT\n" +
                "base.id AS city_id,\n" +
                "base.name AS city_name,\n" +
                "IF\n" +
                "(cg.city_cluster is null, concat( bp.name, '群' ), cg.city_cluster ) AS city_cluster,\n" +
                "bp.id AS parent_id,\n" +
                "bp.name AS parent_name \n" +
                "FROM\n" +
                "source_base_geo_info base\n" +
                "LEFT JOIN source_user_dispatch_city_group cg ON cg.city_id = base.id\n" +
                "LEFT JOIN source_base_geo_info bp ON bp.id = base.parent_id \n" +
                "WHERE\n" +
                "( cg.city_id IS NOT NULL ) \n" +
                "OR base.type = 3) ccl\n" +
                "on t.c_id = ccl.city_id\n" +
                "WHERE\n" +
                "r.user_tag_id in (10001,10000,10002,10003)) tmp group by puid) ttmp\n" +
                "where release_date >=DATE_FORMAT(TIMESTAMPADD(DAY,-1,CURRENT_TIMESTAMP),'yyyy-MM-dd')");

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
        }).addSink(new FlinkHotTagCdc2.KuduSink());


        env.execute("司机热标签实时表");
    }

    public static class KuduSink extends RichSinkFunction<Row> {
        private KuduTable table = null;
        private KuduClient client = null;
        private  KuduSession session = null;
        private  String masteraddr = "172.27.16.41:7051,172.27.16.43:7051,172.27.16.49:7051";
        @Override
        public void open(Configuration parameters) throws Exception {
            //创建kudu连接
            client = new KuduClient.KuduClientBuilder(masteraddr).defaultSocketReadTimeoutMs(6000).build();
            //打开表
            table = client.openTable("stage.stage_driver_hot_tag_3d");
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
                String puid = value.getField(0).toString();
                String tag_type = value.getField(1).toString();
                String create_time = value.getField(2).toString();
                String release_date = value.getField(3).toString();
                String empty_city_id = value.getField(4).toString();
                String empty_city_cluster = value.getField(5).toString();
                String intention_city_id = value.getField(6).toString();
                String intention_city_cluster = value.getField(7).toString();
                String ban_push_time = value.getField(8).toString();
                String aa = "";
                String regEx = "(=1| |\\{|\\})";
                session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
                session.setTimeoutMillis(60000);
                Upsert upsert = table.newUpsert();
                PartialRow row2 = upsert.getRow();
                row2.addString("puid", puid);
                row2.addString("create_time", create_time);
                row2.addString("tag_type", tag_type);
                row2.addString("release_date", release_date);
//                row2.addString("empty_city_id", empty_city_id);
//                row2.addString("empty_city_cluster", empty_city_cluster);
//                row2.addString("ban_push_time", ban_push_time);
//                String release_date1 = release_date.replaceAll(regEx,aa);
//                String release_date2 = release_date1.substring(1);
                if (" ".equals(ban_push_time)) {
                    row2.addString("ban_push_time", ban_push_time);
                }
//                System.out.println("ban_push_time"+release_date);
                String empty_city_id1 = empty_city_id.replaceAll(regEx, aa);
//                System.out.println(getType(empty_city_id1))
                if (empty_city_id1.indexOf(",") == 0) {
                    String empty_city_id2 = empty_city_id1.substring(1);
                    row2.addString("empty_city_id", empty_city_id2);
                } else if ((empty_city_id1.lastIndexOf(',') != -1) && (empty_city_id1.lastIndexOf(',') + 1 == empty_city_id1.length())) {
                    String empty_city_id2 = empty_city_id1.substring(0, empty_city_id1.length() - 1);
                    row2.addString("empty_city_id", empty_city_id2);
                }

                String empty_city_cluster1 = empty_city_cluster.replaceAll(regEx, aa);
                if (empty_city_cluster1.indexOf(",") == 0) {
                    String empty_city_cluster2 = empty_city_cluster1.substring(1);
                    row2.addString("empty_city_cluster", empty_city_cluster2);
                } else if ((empty_city_cluster1.lastIndexOf(',') != -1) && (empty_city_cluster1.lastIndexOf(',') + 1 == empty_city_cluster1.length())) {
                    String empty_city_cluster2 = empty_city_cluster1.substring(0, empty_city_cluster1.length() - 1);
                    row2.addString("empty_city_cluster", empty_city_cluster2);
                }

                String intention_city_id1 = intention_city_id.replaceAll(regEx, aa);
                String intention_city_cluster1 = intention_city_cluster.replaceAll(regEx, aa);
                if (intention_city_id1.indexOf(",") == 0) {
                    String intention_city_id2 = intention_city_id1.substring(1);
                    row2.addString("intention_city_id", intention_city_id2);
                } else if ((intention_city_id1.lastIndexOf(',') != -1) && (intention_city_id1.lastIndexOf(',') + 1 == intention_city_id1.length())) {
                    String intention_city_id2 = intention_city_id1.substring(0, intention_city_id1.length() - 1);
                    row2.addString("intention_city_id", intention_city_id2);
                } else {
                    row2.addString("intention_city_id", " ");
                }

                if (intention_city_cluster1.indexOf(",") == 0) {
                    String intention_city_cluster2 = intention_city_cluster1.substring(1);
                    row2.addString("intention_city_cluster", intention_city_cluster2);
                } else if ((intention_city_cluster1.lastIndexOf(',') != -1) && (intention_city_cluster1.lastIndexOf(',') + 1 == intention_city_cluster1.length())) {
                    String intention_city_cluster2 = intention_city_cluster1.substring(0, intention_city_cluster1.length() - 1);
                    row2.addString("intention_city_cluster", intention_city_cluster2);
                } else {
                    row2.addString("intention_city_cluster", " ");
                }

                String ban_push_time1 = ban_push_time.replaceAll(regEx, aa);

                if (ban_push_time1.indexOf(",") == 0) {
                    String ban_push_time2 = ban_push_time1.substring(1);
                    row2.addString("ban_push_time", ban_push_time2);
                } else if ((ban_push_time1.lastIndexOf(',') != -1) && (ban_push_time1.lastIndexOf(',') + 1 == ban_push_time1.length())) {
                    String ban_push_time2 = ban_push_time1.substring(0, ban_push_time1.length() - 1);
                    row2.addString("ban_push_time", ban_push_time2);
                } else {
                    row2.addString("ban_push_time", " ");
                }
//                System.out.println(value.toString());
                logger.info(value.toString());
                session.apply(upsert);
                session.flush();
            } catch (Exception e) {
//                e.printStackTrace();
//                logger.debug(e.getMessage(),e);
                logger.error(e.getMessage(),e);
//                System.out.println("Wrong!");
            }
//            System.out.println("Done Successfully!");
        }
    }
}

