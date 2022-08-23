package com.example.cdc;
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

public class FlinkDriverCdc1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_base_geo_info (id bigint,parent_id bigint,name string,type int,proc_time AS PROCTIME() ) with(" +
                " 'connector' = 'jdbc', " +
                "'driver'='com.mysql.cj.jdbc.Driver',"+
                "  'username' = 'flink_cdc_ad', " +
                "  'password' = 'jklsek52@=9H', " +
                "  'table-name' = 'base_geo_info', " +
                " 'url' = 'jdbc:mysql://172.27.0.48:3306/ht_travel'," +
                " 'lookup.cache.max-rows' = '1000'," +
                " 'lookup.cache.ttl' = '10000'"+
                ")");

//        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_base_geo_info (id bigint,parent_id bigint,name string,type int) with(" +
//                " 'connector' = 'mysql-cdc', " +
//                " 'hostname' = '172.27.0.48', " +
//                " 'server-id' = '20004', " +
//                "  'port' = '3306', " +
//                "  'username' = 'flink_cdc_ad', " +
//                "  'password' = 'jklsek52@=9H', " +
//                "  'database-name' = 'ht_travel', " +
//                "  'table-name' = 'base_geo_info', " +
//                "  'server-time-zone' = 'Asia/Shanghai', " +
//                "  'debezium.snapshot.locking.mode' = 'none', " +
//                "  'scan.incremental.snapshot.enabled' = 'false', "+
//                "  'debezium.snapshot.mode' = 'initial' " +
//                ")");


        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_wx_external_platform_mapping (id bigint,external_user_id string,mobile string,type int,platform_u_id string,create_time string,update_time string) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '41001', " +
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
                " 'server-id' = '41002', " +
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


        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_user_dispatch_city_group (city_id bigint,city_cluster string,proc_time AS PROCTIME() ) with(" +
                " 'connector' = 'jdbc', " +
                "'driver'='com.mysql.cj.jdbc.Driver',"+
                "  'username' = 'flink_cdc_ad', " +
                "  'password' = 'jklsek52@=9H', " +
                "  'table-name' = 'user_dispatch_city_group', " +
                " 'url' = 'jdbc:mysql://172.27.0.48:3306/ht_user'," +
                " 'lookup.cache.max-rows' = '1000'," +
                " 'lookup.cache.ttl' = '10000'"+
                ")");


//        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_user_dispatch_city_group (city_id bigint,city_cluster string) with(" +
//                " 'connector' = 'mysql-cdc', " +
//                " 'hostname' = '172.27.0.48', " +
//                " 'server-id' = '20003', " +
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



        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_wx_tag (id bigint,name string,c_id bigint,status int,type int) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '41005', " +
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





        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_contracted_customer (id bigint,link_id bigint,create_time timestamp,cooperation_status int) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '41006', " +
                "  'port' = '3306', " +
                "  'username' = 'flink_cdc_ad', " +
                "  'password' = 'jklsek52@=9H', " +
                "  'database-name' = 'ht_user', " +
                "  'table-name' = 'contracted_customer', " +
                "  'server-time-zone' = 'Asia/Shanghai', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', "+
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_customer_related_personnel (id bigint,customer_id bigint,corp_user_id bigint,status int) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '41007', " +
                "  'port' = '3306', " +
                "  'username' = 'flink_cdc_ad', " +
                "  'password' = 'jklsek52@=9H', " +
                "  'database-name' = 'ht_user', " +
                "  'table-name' = 'customer_related_personnel', " +
                "  'server-time-zone' = 'Asia/Shanghai', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', "+
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_customer_related_personnel_info (customer_rp_id bigint,status int,firm_name string,customer_id bigint) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '41032', " +
                "  'port' = '3306', " +
                "  'username' = 'flink_cdc_ad', " +
                "  'password' = 'jklsek52@=9H', " +
                "  'database-name' = 'ht_user', " +
                "  'table-name' = 'customer_related_personnel_info', " +
                "  'server-time-zone' = 'Asia/Shanghai', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', "+
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");



        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_qw_third_part_user (id bigint,user_id string,open_user_id string,suite_id string,corp_id string,status int) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '41008', " +
                "  'port' = '3306', " +
                "  'username' = 'flink_cdc_ad', " +
                "  'password' = 'jklsek52@=9H', " +
                "  'database-name' = 'ht_user', " +
                "  'table-name' = 'qw_third_part_user', " +
                "  'server-time-zone' = 'Asia/Shanghai', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', "+
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_wechat_thirdpart_follow_user (id bigint,suite_id string,qw_user_id bigint,owner_id bigint,status int,deleted int,user_id string,remark_mobiles string,oper_user_id string,corp_id string,external_contact_id bigint) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '41009', " +
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

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_wechat_external_contact (external_user_id string,create_time string) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '41010', " +
                "  'port' = '3306', " +
                "  'username' = 'flink_cdc_ad', " +
                "  'password' = 'jklsek52@=9H', " +
                "  'database-name' = 'ht_user', " +
                "  'table-name' = 'wechat_external_contact', " +
                "  'server-time-zone' = 'Asia/Shanghai', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', "+
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");


        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_truck_owners (id bigint,status int) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '41010', " +
                "  'port' = '3306', " +
                "  'username' = 'flink_cdc_ad', " +
                "  'password' = 'jklsek52@=9H', " +
                "  'database-name' = 'ht_user', " +
                "  'table-name' = 'truck_owners', " +
                "  'server-time-zone' = 'Asia/Shanghai', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', "+
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");

        Table tableQuery = tableEnv.sqlQuery("select crpi.customer_id,coalesce(tt.remark_mobiles,f.remark_mobiles) as mobile,crpi.customer_rp_id from\n" +
                "    (select * from source_customer_related_personnel_info where status = 1) crpi \n" +
                "    left join\n" +
                "    (select * from source_customer_related_personnel where status=1) crp\n" +
                "    on crp.id = crpi.customer_rp_id\n" +
                "    left join\n" +
                "    (select * from source_qw_third_part_user where status = 1) qwpu\n" +
                "    on crp.corp_user_id = qwpu.id\n" +
                "    left join\n" +
                "    (select * from source_wechat_thirdpart_follow_user where status=1 and deleted=0 and remark_mobiles is not null and qw_user_id>0 and user_id is not null and suite_id is not null and corp_id is not null\n" +
                "    and char_length(user_id)>0 and char_length(suite_id)>0 and char_length(corp_id)>0) f \n" +
                "     on f.user_id = qwpu.user_id and f.suite_id = qwpu.suite_id and f.corp_id = qwpu.corp_id\n" +
                "    left join \n" +
                "    (  \n" +
                "    select qwpu.id,ff.qw_user_id ,ff.remark_mobiles\n" +
                "    from\n" +
                "    (select id from source_qw_third_part_user where status = 1) qwpu\n" +
                "    inner join \n" +
                "    (select * from source_wechat_thirdpart_follow_user where status=1 and deleted=0 and qw_user_id is not null and remark_mobiles is not null and qw_user_id>0) ff \n" +
                "    on qwpu.id = ff.qw_user_id\n" +
                "    ) tt\n" +
                "    on qwpu.id = tt.id");


//        Table tableQuery = tableEnv.sqlQuery("select crpi.customer_id,ifnull(f.remark_mobiles,tt.remark_mobiles) as remark_mobiles,crpi.customer_rp_id from\n" +
//                "    (select * from source_customer_related_personnel_info where status = 1) crpi \n" +
//                "    left join\n" +
//                "    (select * from source_customer_related_personnel where status=1) crp\n" +
//                "    on crp.id = crpi.customer_rp_id\n" +
//                "    left join \n" +
//                "    (select * from source_qw_third_part_user where status = 1) qwpu \n" +
//                "    on crp.corp_user_id = qwpu.id\n" +
//                "    left join \n" +
//                "    (select * from source_wechat_thirdpart_follow_user where status=1 and deleted=0 and remark_mobiles is not null and qw_user_id>0) f\n" +
//                "    on ((f.user_id = qwpu.user_id and f.suite_id = qwpu.suite_id and f.corp_id = qwpu.corp_id))\n" +
//                "    left join \n" +
//                "     (\n" +
//                "    select qwpu.id,f.remark_mobiles from\n" +
//                "    (select id from source_qw_third_part_user where status = 1) qwpu \n" +
//                "    inner join \n" +
//                "    (select  * from source_wechat_thirdpart_follow_user where status=1 and deleted=0 and remark_mobiles is not null and qw_user_id>0) f\n" +
//                "    on  ((f.qw_user_id = qwpu.id ))\n" +
//                "    ) tt\n" +
//                "    on qwpu.id = tt.id");

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
        }).addSink(new MysqlSink());
        env.execute("货主熟车司机实时表");
    }

    public static class MysqlSink extends RichSinkFunction<Row> {
        PreparedStatement ps;
        private Connection conn;
        private String sql;

        /**
         * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
         *
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("初始化数据库连接池");
            conn = C3P0Util.getConnection();
            conn.setAutoCommit(false);
            boolean ret = false;
            sql = "INSERT into thridpart_driver_info(customer_id,mobile,user_id,empty_city,empty_city_id,release_time,prefer_city) values(?,?,?,null,null,null,null) on DUPLICATE key UPDATE empty_city=null,empty_city_id=null,release_time=null,prefer_city=null";
            ps = this.conn.prepareStatement(sql);
        }

        @Override
        public void close() throws Exception {
            super.close();
            //关闭连接和释放资源
            if (conn != null) {
                conn.close();
            }
            if (ps != null) {
                ps.close();
            }
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
            try {
                int v0 = 0;
                String v1 = "";
                String v2 = "";
//                String v3= "";
//                int v4= 0;
//                String v5= "";
//                String v6= "";

                String[] value1 = new String[3];
                if(null!=value.getField(0)){
                    v0=Integer.parseInt(value.getField(0).toString());
                }
                System.out.println(v0);
                if(null!=value.getField(1)){
                    v1=value.getField(1).toString();
                }
                System.out.println(v1);
                if(null!=value.getField(2)){
                    v2=value.getField(2).toString();
                }
                System.out.println(v2);
//                if(null!=value.getField(3)){
//                    v3=value.getField(3).toString();
//                }
//                System.out.println(v3);
//                if(null!=value.getField(4)){
//                    v4=Integer.parseInt(value.getField(4).toString().split(",")[0]);
//                }
//                System.out.println(v4);
//                if(null!=value.getField(5)){
//                    v5=value.getField(5).toString();
//                }
//                System.out.println(v5);
//                if(null!=value.getField(6)){
//                    v6=value.getField(6).toString();
//                }
//                System.out.println(v6);
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("v0", v0);
                jsonObject.put("v1", v1);
                jsonObject.put("v2", v2);
//                jsonObject.put("v3", v3);
//                jsonObject.put("v4", v4);
//                jsonObject.put("v5", v5);
//                jsonObject.put("v6", v6);
//                jsonObject.put("v7", v3);
//                jsonObject.put("v8", v4);
//                jsonObject.put("v9", v5);
//                jsonObject.put("v10", v6);
                System.out.println(jsonObject);
                C3P0Util.insertOrUpdateData(sql,jsonObject);
            } catch (Exception e) {
//                System.out.println(e.getMessage());
            } finally {
                System.out.println("["+now_date+"] :成功地插入了1行数据, "+value.toString());
            }
        }
    }
}
