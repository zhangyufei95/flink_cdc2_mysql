package com.example.cdc;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Date;
import java.text.SimpleDateFormat;

import java.sql.Connection;
import java.sql.PreparedStatement;

import static java.sql.JDBCType.NULL;

public class FlinkDriverCdc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setIdleStateRetentionTime(Time.hours(24), Time.hours(25));

//        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_base_geo_info (id bigint,parent_id bigint,name string,type int,proc_time AS PROCTIME() ) with(" +
//                " 'connector' = 'jdbc', " +
//                "'driver'='com.mysql.cj.jdbc.Driver',"+
//                "  'username' = 'flink_cdc_ad', " +
//                "  'password' = 'jklsek52@=9H', " +
//                "  'table-name' = 'base_geo_info', " +
//                " 'url' = 'jdbc:mysql://172.27.0.48:3306/ht_travel'," +
//                " 'lookup.cache.max-rows' = '1000'," +
//                " 'lookup.cache.ttl' = '10000'"+
//                ")");

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


//        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_wx_external_platform_mapping (id bigint,external_user_id string,mobile string,type int,platform_u_id string,create_time string,update_time string) with(" +
//                " 'connector' = 'mysql-cdc', " +
//                " 'hostname' = '172.27.0.48', " +
//                " 'server-id' = '71001', " +
//                "  'port' = '3306', " +
//                "  'username' = 'flink_cdc_ad', " +
//                "  'password' = 'jklsek52@=9H', " +
//                "  'database-name' = 'ht_user', " +
//                "  'table-name' = 'wx_external_platform_mapping', " +
//                "  'server-time-zone' = 'Asia/Shanghai', " +
//                "  'debezium.snapshot.locking.mode' = 'none', " +
//                "  'scan.incremental.snapshot.enabled' = 'false', "+
//                "  'debezium.snapshot.mode' = 'initial' " +
//                ")");
//
//        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_wx_tag_relation (tag_id bigint,user_tag_id bigint,create_user_id bigint,foreign_id bigint,status int,type int,create_time timestamp,update_time timestamp) with(" +
//                " 'connector' = 'mysql-cdc', " +
//                " 'hostname' = '172.27.0.48', " +
//                " 'server-id' = '51002', " +
//                "  'port' = '3306', " +
//                "  'username' = 'flink_cdc_ad', " +
//                "  'password' = 'jklsek52@=9H', " +
//                "  'database-name' = 'ht_user', " +
//                "  'table-name' = 'wx_tag_relation', " +
//                "  'server-time-zone' = 'Asia/Shanghai', " +
//                "  'debezium.snapshot.locking.mode' = 'none', " +
//                "  'scan.incremental.snapshot.enabled' = 'false', "+
//                "  'debezium.snapshot.mode' = 'initial' " +
//                ")");
//
//
//        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_user_dispatch_city_group (city_id bigint,city_cluster string,proc_time AS PROCTIME() ) with(" +
//                " 'connector' = 'jdbc', " +
//                "'driver'='com.mysql.cj.jdbc.Driver',"+
//                "  'username' = 'flink_cdc_ad', " +
//                "  'password' = 'jklsek52@=9H', " +
//                "  'table-name' = 'user_dispatch_city_group', " +
//                " 'url' = 'jdbc:mysql://172.27.0.48:3306/ht_user'," +
//                " 'lookup.cache.max-rows' = '1000'," +
//                " 'lookup.cache.ttl' = '10000'"+
//                ")");


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



//        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_wx_tag (id bigint,name string,c_id bigint,status int,type int) with(" +
//                " 'connector' = 'mysql-cdc', " +
//                " 'hostname' = '172.27.0.48', " +
//                " 'server-id' = '51005', " +
//                "  'port' = '3306', " +
//                "  'username' = 'flink_cdc_ad', " +
//                "  'password' = 'jklsek52@=9H', " +
//                "  'database-name' = 'ht_user', " +
//                "  'table-name' = 'wx_tag', " +
//                "  'server-time-zone' = 'Asia/Shanghai', " +
//                "  'debezium.snapshot.locking.mode' = 'none', " +
//                "  'scan.incremental.snapshot.enabled' = 'false', "+
//                "  'debezium.snapshot.mode' = 'initial' " +
//                ")");
//
//
//
//
//
//        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_contracted_customer (id bigint,link_id bigint,create_time timestamp,cooperation_status int) with(" +
//                " 'connector' = 'mysql-cdc', " +
//                " 'hostname' = '172.27.0.48', " +
//                " 'server-id' = '51006', " +
//                "  'port' = '3306', " +
//                "  'username' = 'flink_cdc_ad', " +
//                "  'password' = 'jklsek52@=9H', " +
//                "  'database-name' = 'ht_user', " +
//                "  'table-name' = 'contracted_customer', " +
//                "  'server-time-zone' = 'Asia/Shanghai', " +
//                "  'debezium.snapshot.locking.mode' = 'none', " +
//                "  'scan.incremental.snapshot.enabled' = 'false', "+
//                "  'debezium.snapshot.mode' = 'initial' " +
//                ")");
//
//        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_customer_related_personnel (id bigint,customer_id bigint,corp_user_id bigint,status int) with(" +
//                " 'connector' = 'mysql-cdc', " +
//                " 'hostname' = '172.27.0.48', " +
//                " 'server-id' = '51007', " +
//                "  'port' = '3306', " +
//                "  'username' = 'flink_cdc_ad', " +
//                "  'password' = 'jklsek52@=9H', " +
//                "  'database-name' = 'ht_user', " +
//                "  'table-name' = 'customer_related_personnel', " +
//                "  'server-time-zone' = 'Asia/Shanghai', " +
//                "  'debezium.snapshot.locking.mode' = 'none', " +
//                "  'scan.incremental.snapshot.enabled' = 'false', "+
//                "  'debezium.snapshot.mode' = 'initial' " +
//                ")");
//
//        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_customer_related_personnel_info (customer_rp_id bigint,status int,firm_name string,customer_id bigint) with(" +
//                " 'connector' = 'mysql-cdc', " +
//                " 'hostname' = '172.27.0.48', " +
//                " 'server-id' = '51032', " +
//                "  'port' = '3306', " +
//                "  'username' = 'flink_cdc_ad', " +
//                "  'password' = 'jklsek52@=9H', " +
//                "  'database-name' = 'ht_user', " +
//                "  'table-name' = 'customer_related_personnel_info', " +
//                "  'server-time-zone' = 'Asia/Shanghai', " +
//                "  'debezium.snapshot.locking.mode' = 'none', " +
//                "  'scan.incremental.snapshot.enabled' = 'false', "+
//                "  'debezium.snapshot.mode' = 'initial' " +
//                ")");



//        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_qw_third_part_user (id bigint,user_id string,open_user_id string,suite_id string,corp_id string,status int) with(" +
//                " 'connector' = 'mysql-cdc', " +
//                " 'hostname' = '172.27.0.48', " +
//                " 'server-id' = '51008', " +
//                "  'port' = '3306', " +
//                "  'username' = 'flink_cdc_ad', " +
//                "  'password' = 'jklsek52@=9H', " +
//                "  'database-name' = 'ht_user', " +
//                "  'table-name' = 'qw_third_part_user', " +
//                "  'server-time-zone' = 'Asia/Shanghai', " +
//                "  'debezium.snapshot.locking.mode' = 'none', " +
//                "  'scan.incremental.snapshot.enabled' = 'false', "+
//                "  'debezium.snapshot.mode' = 'initial' " +
//                ")");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_wechat_thirdpart_follow_user (id bigint,suite_id string,qw_user_id bigint,owner_id bigint,status int,deleted int,user_id string,remark_mobiles string,oper_user_id string,corp_id string,external_contact_id bigint,from_type int) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '71009', " +
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

//        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_wechat_external_contact (external_user_id string,create_time string) with(" +
//                " 'connector' = 'mysql-cdc', " +
//                " 'hostname' = '172.27.0.48', " +
//                " 'server-id' = '51010', " +
//                "  'port' = '3306', " +
//                "  'username' = 'flink_cdc_ad', " +
//                "  'password' = 'jklsek52@=9H', " +
//                "  'database-name' = 'ht_user', " +
//                "  'table-name' = 'wechat_external_contact', " +
//                "  'server-time-zone' = 'Asia/Shanghai', " +
//                "  'debezium.snapshot.locking.mode' = 'none', " +
//                "  'scan.incremental.snapshot.enabled' = 'false', "+
//                "  'debezium.snapshot.mode' = 'initial' " +
//                ")");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_driver_info (moblie string,car_type string,car_length decimal(10,2),route string) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '71010', " +
                "  'port' = '3306', " +
                "  'username' = 'flink_cdc_ad', " +
                "  'password' = 'jklsek52@=9H', " +
                "  'database-name' = 'ht_user', " +
                "  'table-name' = 'driver_info', " +
                "  'server-time-zone' = 'Asia/Shanghai', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', "+
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  ods_car_type (car_type_re string,car_type string,car_type_id bigint,proc_time AS PROCTIME() ) with(" +
                " 'connector' = 'jdbc', " +
                "'driver'='com.mysql.cj.jdbc.Driver',"+
                "  'username' = 'root', " +
                "  'password' = 'Zhicang@2017', " +
                "  'table-name' = 'car_type_re_table', " +
                " 'url' = 'jdbc:mysql://172.27.16.38:3306/algorithm'," +
                " 'lookup.cache.max-rows' = '1000'," +
                " 'lookup.cache.ttl' = '10000'"+
                ")");

//        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_truck_owners (id bigint,status int) with(" +
//                " 'connector' = 'mysql-cdc', " +
//                " 'hostname' = '172.27.0.48', " +
//                " 'server-id' = '51010', " +
//                "  'port' = '3306', " +
//                "  'username' = 'flink_cdc_ad', " +
//                "  'password' = 'jklsek52@=9H', " +
//                "  'database-name' = 'ht_user', " +
//                "  'table-name' = 'truck_owners', " +
//                "  'server-time-zone' = 'Asia/Shanghai', " +
//                "  'debezium.snapshot.locking.mode' = 'none', " +
//                "  'scan.incremental.snapshot.enabled' = 'false', "+
//                "  'debezium.snapshot.mode' = 'initial' " +
//                ")");

        Table tableQuery = tableEnv.sqlQuery("select a.qw_user_id,a.remark_mobiles,a.deleted,ifnull(c.car_type_id,b.car_type) as car_type,b.car_length,b.route from\n" +
                "(select * from source_wechat_thirdpart_follow_user where from_type =2) a \n" +
                "left join \n" +
                "(select * from source_driver_info) b \n" +
                "on a.remark_mobiles = b.moblie \n" +
                "left join \n" +
                "(select car_type_re,cast(car_type_id as string) as car_type_id from ods_car_type) c \n" +
                "on if(b.car_type not in (0,1,2,3,4,5,6,7,8,9),locate(b.car_type,c.car_type_re)>0,1=0)");

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

    public static String doPost(String url, JSONObject json){
        HttpClient httpClient = new HttpClient();
        PostMethod postMethod = new PostMethod(url);

        postMethod.addRequestHeader("accept", "*/*");
        postMethod.addRequestHeader("connection", "Keep-Alive");
        //设置json格式传送
        postMethod.addRequestHeader("Content-type", "application/x-www-form-urlencoded");
        //必须设置下面这个Header
        postMethod.addRequestHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.81 Safari/537.36");
        //添加请求参数
        postMethod.addParameter("mode", json.getString("mode"));

        String res = "";
        try {
            int code = httpClient.executeMethod(postMethod);
            System.out.println(code);
            if (code == 200){
                res = postMethod.getResponseBodyAsString();
                System.out.println(res);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return res;
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
            sql = "INSERT into thridpart_driver_info(customer_id,mobile,user_id,empty_city,empty_city_id,release_time,prefer_city,is_delete,car_type,car_length,route) values(?,?,null,null,null,null,null,?,?,?,?) ON DUPLICATE KEY UPDATE is_delete = ?,car_type = ?,car_length = ?,route = ?";
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
            System.out.println(value.toString());
            try {

                String v0 = "";
                String v1 = "";
                int v2 = 0;
                String v3 = "";
                String v4 = "";
                String v5 = "";


                String[] value1 = new String[3];
                if(null!=value.getField(0)){
                    v0=value.getField(0).toString();
                }
                if(null!=value.getField(1)){
                    v1=value.getField(1).toString();
                }
                if(null!=value.getField(2)){
                    v2=Integer.parseInt(value.getField(2).toString());
                }

                if(null!=value.getField(3)){
                    v3=value.getField(3).toString();
                }

                if(null!=value.getField(4)){
                    v4=value.getField(4).toString();
                }

                if(null!=value.getField(5)){
                    v5=value.getField(5).toString();
                }

                v5 = "'"+v5+"'";


                JSONObject jsonObject = new JSONObject();
                jsonObject.put("v0", v0);
                jsonObject.put("v1", v1);
                jsonObject.put("v2", v2);
                jsonObject.put("v3", v3);
                jsonObject.put("v4", v4);
                jsonObject.put("v5", v5);
                System.out.println(jsonObject);

                JSONObject jsonObject1 = new JSONObject();
                jsonObject1.put("mode", "short");
                //鑫哥biss-正是环境
//                System.out.println(doPost("https://gw.heptax.com/algo/recommend/user_similar_recomm_init", jsonObject1));
                //鑫哥biss-测试环境
//                System.out.println(doPost("https://gate.heptax.com/algo/recommend/user_similar_recomm_init", jsonObject1));
//                System.out.println(value.toString());
//                C3P0Util.insertOrUpdateData(sql,jsonObject);
            } catch (Exception e) {
//                System.out.println(e.getMessage());
            } finally {
//                System.out.println("["+now_date+"] :成功地插入了1行数据, "+value.toString());
            }
        }
    }
}
