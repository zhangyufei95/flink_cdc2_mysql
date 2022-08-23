package com.example.cdc;

import com.alibaba.fastjson.JSONArray;
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

public class FlinkHotTagCdc30 {
    private static final Logger logger = Logger.getLogger(FlinkHotTagCdc2.class);
    public static void main(String[] args) throws Exception {
        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);



        //流转表
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

//        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_user_dispatch_city_group (city_id bigint,city_cluster string) with(" +
//                " 'connector' = 'mysql-cdc', " +
//                " 'hostname' = '172.27.0.48', " +
//                " 'server-id' = '49917', " +
//                "  'port' = '3306', " +
//                "  'username' = 'flink_cdc_ad', " +
//                "  'password' = 'jklsek52@=9H', " +
//                "  'database-name' = 'ht_user', " +
//                "  'table-name' = 'user_dispatch_city_group', " +
//                "  'server-time-zone' = 'Asia/Shanghai', " +
//                "  'debezium.snapshot.locking.mode' = 'none', " +
//                "  'scan.incremental.snapshot.enabled' = 'false', "+
//                "  'debezium.snapshot.mode' = 'initial' " +
//                ")");

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

        Table tableQuery = tableEnv.sqlQuery("select\n" +
                "platform_u_id,\n" +
                "tag_type,\n" +
                "create_time,\n" +
                "max(release_date),\n" +
                "ifnull(empty_city_id,0),\n" +
                "ifnull(empty_city_cluster,''),\n" +
                "collect(distinct c_id)  as intention_city_id,\n" +
                "collect(distinct city_cluster) as intention_city_cluster,\n" +
                "'' as ban_push_time\n" +
                "from\n" +
                "(SELECT m.platform_u_id,\n" +
                "       from_unixtime(cast(cast(r1.name as bigint)/1000 as int),'yyyy-MM-dd') as release_date ,\n" +
                "       if(if(r1.create_time>r1.create_time,r1.create_time,r1.create_time)>r1.create_time,if(r1.create_time>r1.create_time,r1.create_time,r1.create_time),r1.create_time) as create_time ,\n" +
                "       if(if(if(r1.create_user_id>r1.create_user_id,r1.create_user_id,r1.create_user_id)>r1.create_user_id,if(r1.create_user_id>r1.create_user_id,r1.create_user_id,r1.create_user_id),r1.create_user_id)<776, 'algo', 'other') AS tag_type,\n" +
                "       r2.c_id as empty_city_id,\n" +
                "       r2.city_cluster as empty_city_cluster,\n" +
                "       r3.c_id,\n" +
                "       r3.city_cluster\n" +
                "FROM source_wx_external_platform_mapping m\n" +
                "inner JOIN\n" +
                "  (SELECT foreign_id,\n" +
                "          tag_id,\n" +
                "          t.name,\n" +
                "          r.create_time,\n" +
                "          r.create_user_id,\n" +
                "          r.user_tag_id\n" +
                "   FROM source_wx_tag_relation r\n" +
                "   JOIN source_wx_tag t ON r.tag_id=t.id\n" +
                "   AND t.status=1\n" +
                "   WHERE r.user_tag_id IN(10000)\n" +
                "     AND r.status=1 and from_unixtime(cast(cast(t.name as bigint)/1000 as int),'yyyy-MM-dd')>= DATE_FORMAT(TIMESTAMPADD(DAY,-1,CURRENT_TIMESTAMP),'yyyy-MM-dd') )r1 ON m.id=r1.foreign_id\n" +
                "LEFT JOIN\n" +
                "  (SELECT foreign_id,\n" +
                "          tag_id,\n" +
                "          t.c_id,\n" +
                "          r.create_time,\n" +
                "          r.create_user_id,\n" +
                "          city_cluster\n" +
                "   FROM source_wx_tag_relation r\n" +
                "   JOIN source_wx_tag t ON r.tag_id=t.id\n" +
                "   AND t.status=1\n" +
                "   LEFT JOIN\n" +
                "     (SELECT base.id AS city_id,\n" +
                "             base.name AS city_name,\n" +
                "             concat(bp.name,'群') as city_cluster,\n" +
//                "             ifnull(cg.city_cluster,\n" +
//                "                 concat(bp.name, '群')) AS city_cluster,\n" +
                "                bp.id AS parent_id,\n" +
                "                bp.name AS parent_name\n" +
                "      FROM source_base_geo_info base\n" +
                //  "      LEFT JOIN source_user_dispatch_city_group cg ON cg.city_id = base.id\n" +
                "      LEFT JOIN source_base_geo_info bp ON bp.id = base.parent_id\n" +
                "       WHERE base.type=3) ccl ON t.c_id = ccl.city_id\n " +
//                "      WHERE base.type=3) ccl ON t.c_id = ccl.city_id\n"  +
//                "      WHERE (cg.city_id IS NOT NULL)\n" +
//                "        OR base.type = 3) ccl ON t.c_id = ccl.city_id\n" +
                "   WHERE r.user_tag_id IN(10001)\n" +
                "     AND r.status=1)r2 ON m.id=r2.foreign_id\n" +
                "LEFT JOIN\n" +
                "  (SELECT foreign_id,\n" +
                "          tag_id,\n" +
                "          t.c_id,\n" +
                "          r.create_time,\n" +
                "          r.create_user_id,\n" +
                "          city_cluster\n" +
                "   FROM source_wx_tag_relation r\n" +
                "   JOIN source_wx_tag t ON r.tag_id=t.id\n" +
                "   LEFT JOIN\n" +
                "     (SELECT base.id AS city_id,\n" +
                "             base.name AS city_name,\n" +
                "             ifnull(cg.city_cluster,\n" +
                "                 concat(bp.name, '群')) AS city_cluster,\n" +
                "                bp.id AS parent_id,\n" +
                "                bp.name AS parent_name\n" +
                "      FROM source_base_geo_info base\n" +
                "      LEFT JOIN source_user_dispatch_city_group cg ON cg.city_id = base.id\n" +
                "      LEFT JOIN source_base_geo_info bp ON bp.id = base.parent_id\n" +
                "      WHERE (cg.city_id IS NOT NULL)\n" +
                "        OR base.type = 3) ccl ON t.c_id = ccl.city_id\n" +
                "   AND t.status=1\n" +
                "   WHERE r.user_tag_id IN(10002)\n" +
                "     AND r.status=1)r3 ON m.id=r3.foreign_id\n" +
                "WHERE m.platform_u_id IS NOT NULL\n" +
                "  and  from_unixtime(cast(cast(r1.name as bigint)/1000 as int),'yyyy-MM-dd')>= DATE_FORMAT(TIMESTAMPADD(DAY,-1,CURRENT_TIMESTAMP),'yyyy-MM-dd')\n" +
                "  ) end_table\n" +
                "group by platform_u_id,tag_type,create_time,empty_city_id,empty_city_cluster");

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
        }).addSink(new FlinkHotTagCdc3.KuduSink());




        env.execute("司机热标签实时表");
    }

    public static class KuduSink extends RichSinkFunction<Row> {
        private KuduTable table = null;
        private KuduClient client = null;
        private  KuduSession session = null;
        private int key_state = 0;
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
            key_state = key_state + 1;
            JSONArray jsonArray = new JSONArray();
            String aa = "";
            String regEx = "(=1| |\\{|\\})";
            while (key_state<=10) {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("puid",value.getField(0).toString());
                jsonObject.put("tag_type",value.getField(1).toString());
                jsonObject.put("create_time",value.getField(2).toString());
                jsonObject.put("release_date",value.getField(3).toString());
                jsonObject.put("empty_city_id",value.getField(4).toString());
                jsonObject.put("empty_city_cluster",value.getField(5).toString());

                String intention_city_id = value.getField(6).toString();
                String intention_city_cluster = value.getField(7).toString();

                String intention_city_id1 = intention_city_id.replaceAll(regEx, aa);
                String intention_city_cluster1 = intention_city_cluster.replaceAll(regEx, aa);
                if (intention_city_id1.indexOf(",") == 0) {
                    String intention_city_id2 = intention_city_id1.substring(1);
                    jsonObject.put("intention_city_id", intention_city_id2);
                } else if ((intention_city_id1.lastIndexOf(',') != -1) && (intention_city_id1.lastIndexOf(',') + 1 == intention_city_id1.length())) {
                    String intention_city_id2 = intention_city_id1.substring(0, intention_city_id1.length() - 1);
                    jsonObject.put("intention_city_id", intention_city_id2);
                } else {
                    jsonObject.put("intention_city_id", " ");
                }

                if (intention_city_cluster1.indexOf(",") == 0) {
                    String intention_city_cluster2 = intention_city_cluster1.substring(1);
                    jsonObject.put("intention_city_cluster", intention_city_cluster2);
                } else if ((intention_city_cluster1.lastIndexOf(',') != -1) && (intention_city_cluster1.lastIndexOf(',') + 1 == intention_city_cluster1.length())) {
                    String intention_city_cluster2 = intention_city_cluster1.substring(0, intention_city_cluster1.length() - 1);
                    jsonObject.put("intention_city_cluster", intention_city_cluster2);
                } else {
                    jsonObject.put("intention_city_cluster", " ");
                }
                jsonObject.put("ban_push_time"," ");
            }
            try {

                session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
                session.setTimeoutMillis(60000);

                logger.info(value.toString());
                session.flush();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }
}

