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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class FlinkHotTagCdc1 {
    public static void main(String[] args) throws Exception {
        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_wx_external_platform_mapping (id bigint,type int,platform_u_id string) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '6915', " +
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
                " 'server-id' = '6916', " +
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
                " 'server-id' = '6917', " +
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
                " 'server-id' = '6918', " +
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
                " 'server-id' = '6919', " +
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

        Table tableQuery = tableEnv.sqlQuery("select puid,create_user_id as tag_type,create_time,release_date,empty_city_id,empty_city_cluster,intention_city_id,intention_city_cluster,ban_push_time from (SELECT\n" +
                "m.platform_u_id as puid,\n" +
                "date_format(r.create_time,'yyyy-MM-dd HH:mm:ss') as create_time,\n" +
                "r.create_user_id,\n" +
                "if(r.user_tag_id=10000,from_unixtime(cast(cast(t.name as bigint)/1000 as int),'yyyy-MM-dd'),' ') as release_date,\n" +
                "if(r.user_tag_id=10001,cast(t.c_id as string),' ') as empty_city_id,\n" +
                "if(r.user_tag_id=10001,ccl.city_cluster,' ') as   empty_city_cluster,\n" +
                "if(r.user_tag_id=10002,cast(t.c_id as string),' ') as intention_city_id,\n" +
                "if(r.user_tag_id=10002,ccl.city_cluster,' ') as   intention_city_cluster,\n" +
                "if(r.user_tag_id=10003,from_unixtime(cast(cast(t.name as bigint)/1000 as int),'yyyy-MM-dd'),' ') as ban_push_time\n" +
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
                "r.user_tag_id in (10001,10000,10002,10003) and r.status=1) tmp");

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


//                .addSink(new FlinkHotTagCdc.KuduSink());


        env.execute("司机热标签实时表");
    }

    public static class KuduSink extends RichSinkFunction<Row> {
        private final Logger log = LoggerFactory.getLogger(getClass());
        private KuduTable table = null;
        private KuduClient client = null;
        private  KuduSession session = null;
        private  String masteraddr = "172.27.16.41:7051,172.27.16.43:7051,172.27.16.49:7051";
        @Override
        public void open(Configuration parameters) throws Exception {
            //创建kudu连接
            client = new KuduClient.KuduClientBuilder(masteraddr).defaultSocketReadTimeoutMs(6000).build();
            //打开表
            table = client.openTable("stage.stage_driver_hot_tag_3d1");
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
//                if ("7881302613916514".equals(value.getField(0).toString())) {
//                    System.out.println("意向城市群："+value.getField(7).toString());
//                }
                session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
                session.setTimeoutMillis(60000);
                List<String> projectColumns = new ArrayList<String>();
                //添加select 字段名
                projectColumns.add("puid"); //字段名
                projectColumns.add("create_time"); //字段名
                projectColumns.add("tag_type"); //字段名
                projectColumns.add("release_date"); //字段名
                projectColumns.add("empty_city_id"); //字段名
                projectColumns.add("empty_city_cluster"); //字段名
                projectColumns.add("intention_city_cluster"); //字段名
                projectColumns.add("intention_city_cluster"); //字段名
                projectColumns.add("ban_push_time"); //字段名

                String puid = value.getField(0).toString();
                String create_time = value.getField(1).toString();
                String tag_type = value.getField(2).toString();
                String release_date = value.getField(3).toString();
                String empty_city_id = value.getField(4).toString();
                String empty_city_cluster = value.getField(5).toString();
                String intention_city_id = value.getField(6).toString();
                String intention_city_cluster = value.getField(7).toString();
                String ban_push_time = value.getField(8).toString();

                KuduScanner.KuduScannerBuilder builder = client.newScannerBuilder(table).setProjectedColumnNames(projectColumns);

                //比较方法ComparisonOp：GREATER、GREATER_EQUAL、EQUAL、LESS、LESS_EQUAL
                KuduPredicate predicate1 = null;
                predicate1 = predicate1.newComparisonPredicate(table.getSchema().getColumn("puid"),
                        KuduPredicate.ComparisonOp.EQUAL, puid);

                builder.addPredicate(predicate1);
                KuduScanner scanner = builder.build();
                RowResultIterator results = scanner.nextRows();
                int numRows = results.getNumRows();
//                System.out.println(numRows);

//                    System.out.println("实时流中的意向城市:"+intention_city_id+"; "+intention_city_cluster);
                    if (numRows > 0) {
                        while (results.hasNext()) {
                            RowResult result = results.next();
                            if (!(result.isNull(0))) {
                                String puid1 = value.getField(0).toString();
                                String create_time1 = value.getField(1).toString();
                                String tag_type1 = value.getField(2).toString();
                                String release_date1 = value.getField(3).toString();
                                String empty_city_id1 = value.getField(4).toString();
                                String empty_city_cluster1 = value.getField(5).toString();
                                String intention_city_id1 = value.getField(6).toString();
                                String intention_city_cluster1 = value.getField(7).toString();
                                String ban_push_time1 = value.getField(8).toString();

                                System.out.println("查出来的记录: "+value.toString());
                                Update update = table.newUpdate();
                                PartialRow row1 = update.getRow();
                                row1.addString("puid", puid);
                                row1.addString("create_time", value.getField(2).toString());
                                row1.addString("tag_type", value.getField(1).toString());
                                row1.addString("release_date", value.getField(3).toString());
                                row1.addString("empty_city_id", value.getField(4).toString());
                                row1.addString("empty_city_cluster", value.getField(5).toString());
                                row1.addString("ban_push_time", value.getField(8).toString());
                                if (!(" ".equals(intention_city_id1)) && !(" ".equals(intention_city_id))) {
                                    String string_in = intention_city_id1 + "," + intention_city_id;
                                    List list = Arrays.asList(string_in.split(","));
                                    list.removeIf(Objects::isNull);
                                    Set set = new HashSet(list);
                                    String[] rid = (String[]) set.toArray(new String[0]);
                                    String str1 = StringUtils.join(rid, ",");
                                    if (str1.indexOf(" ,") == 0) {
                                        str1 = str1.substring(1);
                                    }
                                    String string_in1 = intention_city_cluster1 + "," + intention_city_cluster;
                                    List list1 = Arrays.asList(string_in1.split(","));
                                    list1.removeIf(Objects::isNull);
                                    Set set1 = new HashSet(list1);
                                    String[] rid1 = (String[]) set1.toArray(new String[0]);
                                    String str2 = StringUtils.join(rid1, ",");
                                    if (str2.indexOf(" ,") == 0) {
                                        str2 = str2.substring(1);
                                    }
                                    row1.addString("intention_city_id", str1);
                                    row1.addString("intention_city_cluster", str2);
                                } else if ((" ".equals(intention_city_id1)) && !(" ".equals(intention_city_id))) {
                                    row1.addString("intention_city_id", intention_city_id);
                                    row1.addString("intention_city_cluster", intention_city_cluster);
                                } else if (!(" ".equals(intention_city_id1)) && (" ".equals(intention_city_id))) {
                                    row1.addString("intention_city_id", " ");
                                    row1.addString("intention_city_cluster", " ");
                                }
//                                System.out.println("再增加热标签为：" + intention_city_cluster);
                                session.apply(update);
                                session.flush();
                            } else {
                                Update update = table.newUpdate();
                                PartialRow row2 = update.getRow();
                                row2.addString("puid", puid);
                                row2.addString("create_time", value.getField(2).toString());
                                row2.addString("tag_type", value.getField(1).toString());
                                row2.addString("release_date", value.getField(3).toString());
                                row2.addString("empty_city_id", value.getField(4).toString());
                                row2.addString("empty_city_cluster", value.getField(5).toString());
                                row2.addString("ban_push_time", value.getField(8).toString());
                                System.out.println(intention_city_id);
                                if (!(" ".equals(intention_city_id))) {
                                    if (intention_city_id.indexOf(" ,") == 0) {
                                        intention_city_id = intention_city_id.substring(1);
                                    }

                                    if (intention_city_cluster.indexOf(" ,") == 0) {
                                        intention_city_cluster = intention_city_cluster.substring(1);
                                    }

                                    row2.addString("intention_city_id", intention_city_id);
                                    row2.addString("intention_city_cluster", intention_city_cluster);
//                                    System.out.println("新增加意向城市为：" + intention_city_cluster);
                                } else {
                                    row2.addString("intention_city_id", " ");
                                    row2.addString("intention_city_cluster", " ");
//                                    System.out.println("删除意向城市");
                                }
                                session.apply(update);
                                session.flush();
                            }
                        }
                    } else {
                        Insert insert = table.newInsert();
                        PartialRow row = insert.getRow();
                        System.out.println("新增加热标签内容为：" + value.toString());
                        if (intention_city_id.indexOf(" ,") == 0) {
                            intention_city_id = intention_city_id.substring(1);
                        }

                        if (intention_city_cluster.indexOf(" ,") == 0) {
                            intention_city_cluster = intention_city_cluster.substring(1);
                        }
                        row.addString("puid", value.getField(0).toString());
                        row.addString("create_time", value.getField(2).toString());
                        row.addString("tag_type", value.getField(1).toString());
                        row.addString("release_date", value.getField(3).toString());
                        row.addString("empty_city_id", value.getField(4).toString());
                        row.addString("empty_city_cluster", value.getField(5).toString());
                        row.addString("intention_city_id", intention_city_id);
                        row.addString("intention_city_cluster", intention_city_cluster);
                        row.addString("ban_push_time", value.getField(8).toString());
                        session.apply(insert);
                        session.flush();
                    }
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Wrong!");
            }
            System.out.println("Done Successfully!");
        }
    }
}

