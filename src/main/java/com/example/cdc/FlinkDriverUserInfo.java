package com.example.cdc;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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

public class FlinkDriverUserInfo {
    public static void main(String[] args) throws Exception {
        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //流转表
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_truck (create_time timestamp,update_time timestamp,owner_id bigint,id bigint,status int,owner string,plate string,main_driver_id bigint,contact_phone string,car_type int,car_full_length decimal(9,3)) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '13331', " +
                "  'port' = '3306', " +
                "  'username' = 'flink_cdc_ad', " +
                "  'password' = 'jklsek52@=9H', " +
                "  'database-name' = 'ht_truck', " +
                "  'table-name' = 'truck', " +
                "  'server-time-zone' = 'Asia/Shanghai', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', " +
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");


        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_wx_external_platform_mapping (create_time string,update_time string,mobile string,id bigint,external_user_id string,type int,platform_u_id string,plate string,truck_id bigint,real_name string) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '300001', " +
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

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_wx_tag_relation (group_key bigint,tag_id bigint,user_tag_id bigint,create_user_id bigint,foreign_id bigint,status int,type int,create_time timestamp,update_time timestamp) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '300002', " +
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

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_wx_tag (id bigint,name string,c_id bigint,status int,type int,car_length decimal(10,2),car_type int) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '300003', " +
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

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_base_geo_info (id bigint,parent_id bigint,name string,type int) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '300004', " +
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

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_qw_third_part_user (name string,id bigint,user_id string,open_user_id string,status int) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '300005', " +
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

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_contracted_customer (id bigint,link_id bigint,link_customer_id bigint,create_time timestamp,cooperation_status int,status int,type int,customer_full_name string,customer_name string,effective_customer int) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '300006', " +
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

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_customer_related_personnel (id bigint,customer_id bigint,corp_user_id bigint,status int,update_time timestamp,create_time timestamp) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '300007', " +
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

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_wechat_thirdpart_follow_user (owner_id bigint,id bigint,user_id string,remark_mobiles string,oper_user_id string,corp_id string,external_contact_id bigint,create_time timestamp,remark string) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '300008', " +
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

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_travel_order (order_id string,truck_id bigint,create_time timestamp,saler_note string,supplier_info string) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '300009', " +
                "  'port' = '3306', " +
                "  'username' = 'flink_cdc_ad', " +
                "  'password' = 'jklsek52@=9H', " +
                "  'database-name' = 'ht_user', " +
                "  'table-name' = 'travel_order', " +
                "  'server-time-zone' = 'Asia/Shanghai', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', "+
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");


        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_travel_order_goods_address (travel_order_id string ,type int,trans_order int ,depart_time string,end_time string,base_geo_id bigint,status int) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '300010', " +
                "  'port' = '3306', " +
                "  'username' = 'flink_cdc_ad', " +
                "  'password' = 'jklsek52@=9H', " +
                "  'database-name' = 'ht_travel', " +
                "  'table-name' = 'travel_order_goods_address', " +
                "  'server-time-zone' = 'Asia/Shanghai', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', "+
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_wechat_external_contact (external_user_id string,create_time string) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '300011', " +
                "  'port' = '3306', " +
                "  'username' = 'flink_cdc_ad', " +
                "  'password' = 'jklsek52@=9H', " +
                "  'database-name' = 'ht_travel', " +
                "  'table-name' = 'wechat_external_contact', " +
                "  'server-time-zone' = 'Asia/Shanghai', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', "+
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_truck_owners (truck_id bigint,name string,mobile string,create_time timestamp,update_time timestamp) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '300011', " +
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


//        Table tableQuery = tableEnv.sqlQuery("select * from (\n" +
//                "select wtfu.remark_mobiles,coalesce(cc.link_id,0),min(concat(qtpu.name,'|',cast(truck.car_type as string),'|',cast(truck.car_full_length as string),'|',truck.plate)) as value1\n" +
//                "        from (select owner_id,remark_mobiles,user_id,remark from source_wechat_thirdpart_follow_user where create_time > '2022-03-01 00:00:00') wtfu\n" +
//                "            left join (\n" +
//                "                select owner_id, split_index(value1,'|',3) as car_type, split_index(value1,'|',4) as car_full_length, split_index(value1,'|',2) as plate\n" +
//                "                from (\n" +
//                "                    select owner_id,\n" +
//                "                    max(concat(cast(coalesce(update_time,create_time) as string),'|',plate,'|',cast(car_type as string),'|',cast(car_full_length as string))) as value1\n" +
//                "                    from source_truck where owner_id>0 \n" +
//                "                    group by owner_id\n" +
//                "                )t\n" +
//                "            ) truck on wtfu.owner_id=truck.owner_id\n" +
//                "            left join (select id,user_id, name from source_qw_third_part_user where `status`  = 1 and CHAR_LENGTH(user_id)>0) qtpu on wtfu.user_id = qtpu.user_id\n" +
//                "            left join (\n" +
//                "                select corp_user_id,cast(split_index(value1,'|',2) as bigint) as customer_id\n" +
//                "                from (\n" +
//                "                    select corp_user_id,\n" +
//                "                    max(concat(cast(coalesce(update_time,create_time) as string),'|',cast(customer_id as string))) as value1\n" +
//                "                    from source_customer_related_personnel\n" +
//                "                    where `status`  = 1 and corp_user_id is not null\n" +
//                "                    group by corp_user_id\n" +
//                "                )t\n" +
//                "            ) crp on crp.corp_user_id = qtpu.id\n" +
//                "            left join (\n" +
//                "                select cc3.id, cc3.link_id, cc4.customer_name, cc4.customer_full_name\n" +
//                "                from (\n" +
//                "                    select cc.id, cc2.link_id\n" +
//                "                    from (select id,link_customer_id from source_contracted_customer where `status` =1) cc\n" +
//                "                    left join (select id,link_id from source_contracted_customer where `status` =1) cc2 on cc.link_customer_id = cc2.id\n" +
//                "                    where coalesce(cc2.link_id,0) > 0\n" +
//                "                ) cc3\n" +
//                "                left join (select link_id,customer_name,customer_full_name from source_contracted_customer where type in (1, 2) and `status`  = 1 and effective_customer = 1) cc4 on cc3.link_id=cc4.link_id\n" +
//                "            ) cc on crp.customer_id = cc.id\n" +
//                "        where wtfu.remark_mobiles is not null\n" +
//                "        group by wtfu.remark_mobiles,coalesce(cc.link_id,0)\n" +
//                "        ) ttt where value1 is not null");

//        Table tableQuery = tableEnv.sqlQuery("select cast(mp.mobile as string) mobile, cast(mp.truck_id as bigint) as truck_id, cast(ufd.car_type as int) car_type, cast(ufd.car_length as decimal(10,2)) as car_length, cast(real_name as string) owner_name\n" +
//                "         , cast(mp.plate as string) plate\n" +
//                "    from (\n" +
//                "        select mobile,split_index(value1,'|',2) as p_u_id,\n" +
//                "               split_index(value1,'|',3) as truck_id,\n" +
//                "               split_index(value1,'|',4) as plate,\n" +
//                "               split_index(value1,'|',5) as real_name from\n" +
//                "        (\n" +
//                "        select mobile,max(concat(cast(coalesce(update_time,create_time) as string),'|',p_u_id,'|',cast(truck_id as string),'|',plate,'|',real_name)) as value1 from (\n" +
//                "        select\n" +
//                "            wepm.id as foreign_id, wepm.external_user_id as e_u_id, wepm.platform_u_id as p_u_id, wepm.truck_id as truck_id, wepm.plate, wepm.real_name\n" +
//                "            , if(CHAR_LENGTH(wepm.mobile)=11,wepm.mobile,'') as mobile,wepm.update_time,wec.create_time\n" +
//                "        from (\n" +
//                "            select t1.*\n" +
//                "            from (\n" +
//                "                select id, external_user_id, platform_u_id, type, truck_id, if(CHAR_LENGTH(plate)<1,'',plate) plate, create_time, update_time, real_name, regexp_replace(mobile,' ','') mobile \n" +
//                "                from source_wx_external_platform_mapping\n" +
//                "                where (type=1 or type=90334) and CHAR_LENGTH(regexp_replace(mobile,' ',''))=11\n" +
//                "            )t1\n" +
//                "        ) wepm\n" +
//                "        left join (select external_user_id,create_time from source_wechat_external_contact) wec on wepm.external_user_id=wec.external_user_id\n" +
//                "        ) tt  group by mobile\n" +
//                "        ) tt2\n" +
//                "    ) mp\n" +
//                "    left join (\n" +
//                "        select p_u_id,  max(car_length) as car_length, max(car_type) as car_type\n" +
//                "        from (\n" +
//                "            select p_u_id, max(car_length) as car_length, max(car_type) as car_type\n" +
//                "            from (\n" +
//                "                SELECT m.platform_u_id as p_u_id, \n" +
//                "                coalesce(r.group_key,0) as group_key\n" +
//                "                , min(if(r.user_tag_id = 10004,car_length,0)) as car_length\n" +
//                "                , min(if(r.user_tag_id = 10004,car_type,0)) as car_type\n" +
//                "                from (select * from source_wx_tag_relation where user_tag_id in(10004) and `status`  = 1) r\n" +
//                "                join (select * from source_wx_tag where `status` =1) t on r.tag_id = t.id\n" +
//                "                join (select * from source_wx_external_platform_mapping where platform_u_id is not null) m on m.id = r.foreign_id\n" +
//                "                group by m.platform_u_id, coalesce(r.group_key,0)\n" +
//                "            )t\n" +
//                "            group by p_u_id) a group by p_u_id\n" +
//                "    ) ufd\n" +
//                "    on mp.p_u_id = ufd.p_u_id");

//        Table tableQuery = tableEnv.sqlQuery("select mobile,split_index(value1,'|',1) as truck_id,split_index(value1,'|',2) as owner_name from \n" +
//                "(\n" +
//                "select mobile,max(concat(cast(coalesce(update_time,create_time) as string),'|',cast(truck_id as string),'|',name)) as value1\n" +
//                "from source_truck_owners where mobile is not null\n" +
//                "group by mobile\n" +
//                ") tt\n");


//        Table tableQuery = tableEnv.sqlQuery("select truck_id, collect(loading_city) as loading_city, collect(unloading_city) as unloading_city,min(load_tm) as load_tm, max(unload_tm) as unload_tm from (\n" +
//                "        select tod.truck_id, collect(loading_city) as loading_city,collect(unloading_city) as unloading_city,min(coalesce(load_tm1,load_tm2,load_tm3)) as load_tm,max(coalesce(last_unloading_tm,unload_tm3,unload_tm2,unload_tm1)) as unload_tm\n" +
//                "        from (\n" +
//                "            select * from source_travel_order where DATE_FORMAT(create_time,'yyyy-MM-dd') >= '2022-06-27' and truck_id > 0 and truck_id not in (1307820744, 2123699994)\n" +
//                "            and if((locate('测试',lower(saler_note))>0 or locate('test',lower(saler_note))>0) or (locate('知藏',lower(supplier_info))>0 or locate('测试',lower(supplier_info))>0 or locate('test',lower(supplier_info))>0 or locate('云柚',lower(supplier_info))>0), 1, 0) = 0\n" +
//                "        ) tod\n" +
//                "        left join (\n" +
//                "        select toga.travel_order_id\n" +
//                "                    , max(if (toga.type = 1 and toga.trans_order = 1, toga.depart_time, '')) as load_tm1\n" +
//                "                    , max(if (toga.type = 2 and toga.trans_order = 1, toga.end_time, '')) as unload_tm1\n" +
//                "                    , max(if (toga.type = 1 and toga.trans_order = 2, toga.depart_time, '')) as load_tm2\n" +
//                "                    , max(if (toga.type = 2 and toga.trans_order = 2, toga.end_time, '')) as unload_tm2\n" +
//                "                    , max(if (toga.type = 1 and toga.trans_order = 3, toga.depart_time, '')) as load_tm3\n" +
//                "                    , max(if (toga.type = 2 and toga.trans_order = 3, toga.end_time, '')) as unload_tm3\n" +
//                "                    , max(toga.end_time) as last_unloading_tm\n" +
//                "                    , max(if (toga.type = 1 and toga.trans_order = 1, cast(toga.base_geo_id as string), '')) as loading_geo_id\n" +
//                "                    , max(if (toga.type = 2 and toga.trans_order = 1, cast(toga.base_geo_id as string), '')) as unloading_geo_id1\n" +
//                "                    , max(if (toga.type = 2 and if(toga.trans_order = 3,true,if(toga.trans_order = 2,true,true)), cast(toga.base_geo_id as string), '')) as unloading_geo_id\n" +
//                "                    , max(if (toga.type = 1 and toga.trans_order = 1, bgi.name, '')) as loading_city\n" +
//                "                    , max(if (toga.type = 2 and toga.trans_order = 1, bgi.name, '')) as unloading_city1\n" +
//                "                    , max(if (toga.type = 2 and if(toga.trans_order = 3,true,if(toga.trans_order = 2,true,true)), bgi.name, '')) as unloading_city\n" +
//                "            from (\n" +
//                "                select * \n" +
//                "                from source_travel_order_goods_address\n" +
//                "                where `status`  = 1\n" +
//                "            ) toga\n" +
//                "            left join source_base_geo_info bgi on toga.base_geo_id = bgi.id\n" +
//                "            group by toga.travel_order_id\n" +
//                "        ) geo\n" +
//                "        on tod.order_id = geo.travel_order_id\n" +
//                "        group by tod.truck_id) a group by truck_id");

        Table tableQuery = tableEnv.sqlQuery("select truck_id, collect(loading_city) as loading_city, collect(unloading_city) as unloading_city,min(load_tm) as load_tm, max(unload_tm) as unload_tm from (\n" +
                "        select tod.truck_id, collect(loading_city) as loading_city,collect(unloading_city) as unloading_city,min(coalesce(load_tm1,load_tm2,load_tm3)) as load_tm,max(coalesce(last_unloading_tm,unload_tm3,unload_tm2,unload_tm1)) as unload_tm\n" +
                "        from (\n" +
                "            select * from source_travel_order where DATE_FORMAT(create_time,'yyyy-MM-dd') >= '2022-06-27' and truck_id > 0 and truck_id not in (1307820744, 2123699994)\n" +
                "            and if((locate('测试',lower(saler_note))>0 or locate('test',lower(saler_note))>0) or (locate('知藏',lower(supplier_info))>0 or locate('测试',lower(supplier_info))>0 or locate('test',lower(supplier_info))>0 or locate('云柚',lower(supplier_info))>0), 1, 0) = 0\n" +
                "        ) tod\n" +
                "        left join (\n" +
                "        select toga.travel_order_id\n" +
                "                    , max(if (toga.type = 1 and toga.trans_order = 1, toga.depart_time, '')) as load_tm1\n" +
                "                    , max(if (toga.type = 2 and toga.trans_order = 1, toga.end_time, '')) as unload_tm1\n" +
                "                    , max(if (toga.type = 1 and toga.trans_order = 2, toga.depart_time, '')) as load_tm2\n" +
                "                    , max(if (toga.type = 2 and toga.trans_order = 2, toga.end_time, '')) as unload_tm2\n" +
                "                    , max(if (toga.type = 1 and toga.trans_order = 3, toga.depart_time, '')) as load_tm3\n" +
                "                    , max(if (toga.type = 2 and toga.trans_order = 3, toga.end_time, '')) as unload_tm3\n" +
                "                    , max(toga.end_time) as last_unloading_tm\n" +
                "                    , max(if (toga.type = 1 and toga.trans_order = 1, cast(toga.base_geo_id as string), '')) as loading_geo_id\n" +
                "                    , max(if (toga.type = 2 and toga.trans_order = 1, cast(toga.base_geo_id as string), '')) as unloading_geo_id1\n" +
                "                    , max(if (toga.type = 2 and if(toga.trans_order = 3,true,if(toga.trans_order = 2,true,true)), cast(toga.base_geo_id as string), '')) as unloading_geo_id\n" +
                "                    , max(if (toga.type = 1 and toga.trans_order = 1, bgi.name, '')) as loading_city\n" +
                "                    , max(if (toga.type = 2 and toga.trans_order = 1, bgi.name, '')) as unloading_city1\n" +
                "                    , max(if (toga.type = 2 and if(toga.trans_order = 3,true,if(toga.trans_order = 2,true,true)), bgi.name, '')) as unloading_city\n" +
                "            from (\n" +
                "                select * \n" +
                "                from source_travel_order_goods_address\n" +
                "                where `status`  = 1\n" +
                "            ) toga\n" +
                "            left join source_base_geo_info bgi on toga.base_geo_id = bgi.id\n" +
                "            group by toga.travel_order_id\n" +
                "        ) geo\n" +
                "        on tod.order_id = geo.travel_order_id\n" +
                "        group by tod.truck_id) a group by truck_id");

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


        env.execute("司机热标签实时表");
    }
}



