package com.example.cdc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kudu.client.*;

import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.*;

public class HtCaller {

    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(HtCaller.class);
    public static void main(String[] args) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String now_time1 = sdf.format(new java.util.Date());
        //设置flink表环境变量
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        //获取flink流环境变量
        StreamExecutionEnvironment exeEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        exeEnv.setParallelism(1);
        //表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(exeEnv, fsSettings);
         //获取tableconfig
//        TableEnvironmentConfig tConfig = tableEnv.getConfig();
        //设置状态清理参数
//        tConfig.setIdleStateRetentionTime(Time.hours(12),Time.hours(24));


//        tableEnv.executeSql(
//                "CREATE TABLE ods_call_in_sobot_detail (\n" +
//                        " id string, \n" +
//                        " demand_id bigint, \n" +
//                        " call_id string, \n" +
//                        " call_type int, \n" +
//                        " tel string, \n" +
//                        " caller string, \n" +
//                        " content string, \n" +
//                        " answer_time timestamp, \n" +
//                        " end_time timestamp, \n" +
//                        " fee_duration int, \n" +
//                        " intention_name string, \n" +
//                        " caller_labels string, \n" +
//                        " call_labels string, \n" +
//                        " status int, \n" +
//                        " create_time timestamp, \n" +
//                        " update_time timestamp, \n" +
//                        " voice_url string, \n" +
//                        " answer_time timestamp\n" +
//                        ") WITH (\n" +
//                        " 'connector' = 'postgres-cdc',\n" +
//                        " 'hostname' = '172.27.0.119',\n" +
//                        " 'port' = '5432',\n" +
//                        " 'username' = 'algo_pgm',\n" +
//                        " 'password' = 'klsy8#$1qsA7',\n" +
//                        " 'database-name' = 'ht_caller',\n" +
//                        " 'schema-name' = 'public',\n" +
//                        " 'debezium.snapshot.mode' = 'initial',\n" +
//                        " 'decoding.plugin.name' = 'pgoutput',\n" +
//                        " 'debezium.slot.name' = 'ht_caller0713',\n" +
//                        " 'table-name' = 'call_in_sobot_detail'\n" +
//                        ")"
//        );
//
//        tableEnv.executeSql(
//                "CREATE TABLE ods_call_sobot_task_detail (\n" +
//                        " id int, \n" +
//                        " demand_id bigint, \n" +
//                        " called_number string, \n" +
//                        " called_name string, \n" +
//                        " mobile string, \n" +
//                        " status int, \n" +
//                        " out_id string, \n" +
//                        " answer_time bigint, \n" +
//                        " end_time int, \n" +
//                        " intention_list string, \n" +
//                        " call_content string, \n" +
//                        " voice_url string, \n" +
//                        " type int, \n" +
//                        " intention_results string, \n" +
//                        " create_time timestamp, \n" +
//                        " update_time timestamp, \n" +
//                        " sobot_task_id string, \n" +
//                        " task_id int \n" +
//                        ") WITH (\n" +
//                        " 'connector' = 'postgres-cdc',\n" +
//                        " 'hostname' = '172.27.0.119',\n" +
//                        " 'port' = '5432',\n" +
//                        " 'username' = 'algo_pgm',\n" +
//                        " 'password' = 'klsy8#$1qsA7',\n" +
//                        " 'database-name' = 'ht_caller',\n" +
//                        " 'schema-name' = 'public',\n" +
//                        " 'debezium.snapshot.mode' = 'initial',\n" +
//                        " 'decoding.plugin.name' = 'pgoutput',\n" +
//                        " 'debezium.slot.name' = 'ht_caller0714',\n" +
//                        " 'table-name' = 'call_sobot_task_detail'\n" +
//                        ")"
//        );
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  ods_driver_info (id int,phone string,IsTeam string,IsStop string,StartCity string,EndCity string,EmptyDate string,CarType int,CarLength double,RasaMessage string,sentence string,intent string,SessionId string,CreateTime string,UpdateTime string,IsTempStop int,SpaceTimeResult int) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.16.38', " +
                " 'server-id' = '21759', " +
                "  'port' = '3306', " +
                "  'username' = 'root', " +
                "  'password' = 'Zhicang@2017', " +
                "  'database-name' = 'algo_nlp', " +
                "  'table-name' = 'driver_info', " +
                "  'server-time-zone' = 'Asia/Shanghai', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', " +
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  ods_non_standard_trans (session_id string,questions_list string,series_no bigint,statusType string,seriesNo string,contentType string,intent string,notes string,question string,create_time string) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.16.38', " +
                " 'server-id' = '21760', " +
                "  'port' = '3306', " +
                "  'username' = 'root', " +
                "  'password' = 'Zhicang@2017', " +
                "  'database-name' = 'algo_nlp', " +
                "  'table-name' = 'non_standard_trans', " +
                "  'server-time-zone' = 'Asia/Shanghai', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', " +
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  ods_driver_info_input (id int,SafePhone string,StartCity string,EndCity string,EmptyDate string,CarType string,CarLength string,intent string,SessionId string,CreateTime string,UpdateTime string,RealPhone string,DemandId string,Goods string) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.16.38', " +
                " 'server-id' = '21763', " +
                "  'port' = '3306', " +
                "  'username' = 'root', " +
                "  'password' = 'Zhicang@2017', " +
                "  'database-name' = 'algo_nlp', " +
                "  'table-name' = 'driver_info_input', " +
                "  'server-time-zone' = 'Asia/Shanghai', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', " +
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  ods_price_log (sessionId string,ask int,bid int,deal tinyint,finish tinyint,price int,update_time string,create_time string,final_intent string,turn int,ask_turn int,msg_fee int,first_ask int,first_bid int,call_type int) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.16.38', " +
                " 'server-id' = '21764', " +
                "  'port' = '3306', " +
                "  'username' = 'root', " +
                "  'password' = 'Zhicang@2017', " +
                "  'database-name' = 'algo_nlp', " +
                "  'table-name' = 'price_log', " +
                "  'server-time-zone' = 'Asia/Shanghai', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', " +
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  ods_contracted_route (id bigint,type int,status int,demand_id bigint,order_shipment int,customer_id bigint,use_car_start timestamp,create_time timestamp) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '21761', " +
                "  'port' = '3306', " +
                "  'username' = 'flink_cdc_ad', " +
                "  'password' = 'jklsek52@=9H', " +
                "  'database-name' = 'ht_user', " +
                "  'table-name' = 'contracted_route', " +
                "  'server-time-zone' = 'Asia/Shanghai', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', " +
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  ods_base_geo_info (id bigint,parent_id bigint,name string,type int,proc_time AS PROCTIME() ) with(" +
                " 'connector' = 'jdbc', " +
                "'driver'='com.mysql.cj.jdbc.Driver',"+
                "  'username' = 'flink_cdc_ad', " +
                "  'password' = 'jklsek52@=9H', " +
                "  'table-name' = 'base_geo_info', " +
                " 'url' = 'jdbc:mysql://172.27.0.48:3306/ht_travel'," +
                " 'lookup.cache.max-rows' = '1000'," +
                " 'lookup.cache.ttl' = '10000'"+
                ")");


        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  ods_contracted_route_info (route_id bigint,type int,sequence_number int,city_id bigint,status int,create_time timestamp) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '21762', " +
                "  'port' = '3306', " +
                "  'username' = 'flink_cdc_ad', " +
                "  'password' = 'jklsek52@=9H', " +
                "  'database-name' = 'ht_user', " +
                "  'table-name' = 'contracted_route_info', " +
                "  'server-time-zone' = 'Asia/Shanghai', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', " +
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");

        Table tableQuery2 = tableEnv.sqlQuery("select * \n" +
                "from\n" +
                "(select demand_id,use_car_start,load_city_name,concat(load_city_name,'-',unload_city_name) as line_prefer from\n" +
                "(select cr.demand_id,cr.use_car_start,cri.load_city_id,geo.name as load_city_name,cri.unload_city_id,geo1.name as unload_city_name from\n" +
                "(select id,demand_id,use_car_start from ods_contracted_route where left(cast(create_time as string),10) >= '2022-07-19' and type=3 and status =1 and demand_id is not null) cr \n" +
                "left join \n" +
                "(select route_id,cast(split_index(min(concat(cast(type as string),'|',cast(sequence_number as string),'|',cast(city_id as string))),'|',2) as bigint) as load_city_id,cast(split_index(max(concat(cast(type as string),'|',cast(sequence_number as string),'|',cast(city_id as string))),'|',2) as bigint) as unload_city_id from ods_contracted_route_info where left(cast(create_time as string),10)>='2022-07-19' and status =1 group by route_id) cri\n" +
                "on cr.id = cri.route_id\n" +
                "left join\n" +
                "(select * from ods_base_geo_info) geo \n" +
                "on cri.load_city_id = geo.id\n" +
                "left join\n" +
                "(select * from ods_base_geo_info) geo1\n" +
                "on cri.unload_city_id = geo1.id\n" +
                ") base_table1 ) base_table");





//        Table tableQuery2 = tableEnv.sqlQuery("select call_table.sessionid,call_table.demand_id,call_table.mobile,\n" +
//                "call_table.voice_url,call_table.intention_results,call_table.answer_time,call_table.call_type,\n" +
//                "dr.IsTeam,dr.IsStop,if(call_table.call_type = 'out',dr.StartCity,'') as startcity,\n" +
//                "if(call_table.call_type = 'out',dr.EndCity,'') as endcity,if(call_table.call_type = 'out',dr.EmptyDate,'') as emptydate,if(call_table.call_type = 'out',cast(dr.CarType as string),dri.CarType) as cartype,if(call_table.call_type = 'out',cast(dr.CarLength as string),dri.CarLength) as carlength,\n" +
//                "if(call_table.call_type = 'out','',dri.Goods) as goods,\n" +
//                "cast(pl.ask as string) as price,non_st.non_st,dr.IsTempStop,DATE_FORMAT(call_table.create_time,'yyyy-MM-dd HH:mm:ss') as create_time,base_table.use_car_start,base_table.load_city_name,base_table.line_prefer\n" +
//                "from\n" +
//                "(select demand_id,from_unixtime(cast(answer_time/1000000 as bigint),'yyyy-MM-dd HH:mm:ss') as answer_time, \n" +
//                "out_id as sessionid,\n" +
//                "called_number as mobile,\n" +
//                "voice_url,\n" +
//                "intention_results,\n" +
//                "'out' as call_type, \n" +
//                "create_time as create_time \n" +
//                "from ods_call_sobot_task_detail where left(cast(create_time as string),10) >= '2022-07-19'\n" +
//                "union all\n" +
//                "select ifnull(demand_id,0) as demand_id,\n" +
//                "cast(answer_time as string) as answer_time,\n" +
//                "call_id as sessionid,\n" +
//                "caller as mobile,\n" +
//                "voice_url,\n" +
//                "intention_name,\n" +
//                "'in' as call_type,\n" +
//                "create_time as create_time \n" +
//                "from ods_call_in_sobot_detail where left(cast(create_time as string),10) >= '2022-07-19'\n" +
//                ") call_table\n" +
//                "left join\n" +
//                "(select demand_id,use_car_start,load_city_name,concat(load_city_name,'-',unload_city_name) as line_prefer from\n" +
//                "(select cr.demand_id,cr.use_car_start,cri.load_city_id,geo.name as load_city_name,cri.unload_city_id,geo1.name as unload_city_name from\n" +
//                "(select id,demand_id,use_car_start from ods_contracted_route where left(cast(create_time as string),10) >= '2022-07-19' and type=3 and status =1 and demand_id is not null) cr \n" +
//                "left join \n" +
//                "(select route_id,cast(split_index(min(concat(cast(type as string),'|',cast(sequence_number as string),'|',cast(city_id as string))),'|',2) as bigint) as load_city_id,cast(split_index(max(concat(cast(type as string),'|',cast(sequence_number as string),'|',cast(city_id as string))),'|',2) as bigint) as unload_city_id from ods_contracted_route_info where left(cast(create_time as string),10)>='2022-07-19' and status =1 group by route_id) cri\n" +
//                "on cr.id = cri.route_id\n" +
//                "left join\n" +
//                "(select * from ods_base_geo_info) geo \n" +
//                "on cri.load_city_id = geo.id\n" +
//                "left join\n" +
//                "(select * from ods_base_geo_info) geo1\n" +
//                "on cri.unload_city_id = geo1.id\n" +
//                ") base_table1 ) base_table\n" +
//                "on call_table.demand_id = base_table.demand_id\n" +
//                "left join\n" +
//                "(select * from ods_driver_info) dr \n" +
//                "on call_table.sessionid = dr.SessionId\n" +
//                "left join\n" +
//                "(select * from ods_driver_info_input) dri\n" +
//                "on call_table.sessionid = dri.SessionId\n" +
//                "left join\n" +
//                "(select * from ods_price_log) pl\n" +
//                "on call_table.sessionid = pl.sessionId \n" +
//                "left join\n" +
//                "(select session_id,collect(concat(notes ,':',intent)) as non_st from ods_non_standard_trans group by session_id) non_st\n" +
//                "on call_table.sessionid = non_st.session_id");

        DataStream<Row> data1 = tableEnv.toRetractStream(tableQuery2, Row.class).filter(
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
        });
        data1.print();
//        data1.addSink(new KuduSink00());
        exeEnv.execute("外呼司机基本信息&热标签&聊价_0719_外呼热标签信息");
    }
    public static class KuduSink00 extends RichSinkFunction<Row> {
        private KuduTable table = null;
        private KuduTable table1 = null;
        private KuduClient client = null;
        private KuduClient client1 = null;
        private KuduSession session = null;
        private String masteraddr = "172.27.16.41:7051,172.27.16.43:7051,172.27.16.49:7051";

        public void open(Configuration parameters) throws Exception {
            //创建kudu连接
            client = new KuduClient.KuduClientBuilder(masteraddr).defaultSocketReadTimeoutMs(6000).build();
            //打开表
            table = client.openTable("stage.stage_call_driver");
            table1 = client.openTable("stage.stage_call_driver_stat");
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
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String now_time = sdf.format(new java.util.Date());
            String sessionid="";
            String demand_id="";
            String mobile = "";
            String record_url = "";
            String intention_results = "";
            String answer_time = "";
            String call_type = "";
            String isTeam = "0";
            String isStop = "0";
            String startCity = "";
            String endCity = "";
            String emptyDate = "";
            String carType = "";
            String carLength = "";
            String goods_name = "";
            String price = "";
            String jsonvalue = "";
            int isTempStop = 0;
            String create_time = "";
            String use_car_start = "";
            String empty_city = "";
            String line_prefer = "";

            try {
                if(null==value.getField(0)){
                    sessionid="";
                } else {
                    sessionid = value.getField(0).toString();
                }
                if(null==value.getField(1)){
                    demand_id="0";
                } else {
                    demand_id = value.getField(1).toString();
                }
                if (null==value.getField(2)){
                    mobile="";
                } else {
                    mobile = value.getField(2).toString();
                }

                if (null==value.getField(3)){
                    record_url="";
                } else {
                    record_url = value.getField(3).toString();
                }

                if (null==value.getField(4)){
                    intention_results="";
                } else {
                    intention_results = value.getField(4).toString();
                }

                if (null==value.getField(5)){
                    answer_time="";
                } else {
                    answer_time = value.getField(5).toString();
                }

                if (null==value.getField(6)){
                    call_type="";
                } else {
                    call_type = value.getField(6).toString();
                }

                if (null==value.getField(7)){
                    isTeam="0";
                } else {
                    isTeam = value.getField(7).toString();
                }

                if (null==value.getField(8)){
                    isStop="0";
                } else {
                    isStop = value.getField(8).toString();
                }

                if (null==value.getField(9)){
                    startCity="";
                } else {
                    startCity = value.getField(9).toString();
                }

                if (null==value.getField(10)){
                    endCity="";
                } else {
                    endCity = value.getField(10).toString();
                }

                if (null==value.getField(11)){
                    emptyDate="";
                } else {
                    emptyDate = value.getField(11).toString();
                }

                if (null==value.getField(12)){
                    carType="";
                } else {
                    carType = value.getField(12).toString();
                }

                if (null==value.getField(13)){
                    carLength="";
                } else {
                    carLength = value.getField(13).toString();
                }

                if (null==value.getField(14)){
                    goods_name="";
                } else {
                    goods_name = value.getField(14).toString();
                }

                if (null==value.getField(15)){
                    price="";
                } else {
                    price = value.getField(15).toString();
                }

                if (null == value.getField(16)) {
                    jsonvalue = "";
                } else {
                    jsonvalue = value.getField(16).toString();
                }
                if (null == value.getField(17)) {
                    isTempStop = 0;
                } else {
                    isTempStop = Integer.parseInt(value.getField(17).toString());
                }

                if (null == value.getField(18)) {
                    create_time = now_time;
                } else {
                    create_time = value.getField(18).toString();
                }

                if (null == value.getField(19)) {
                    use_car_start = "";
                } else {
                    use_car_start = value.getField(19).toString();
                }

                if (null == value.getField(20)) {
                    empty_city = "";
                } else {
                    empty_city = value.getField(20).toString();
                }

                if (null == value.getField(21)) {
                    line_prefer = "";
                } else {
                    line_prefer = value.getField(21).toString();
                }

                JSONObject jsonObject_line_prefer = new JSONObject(new LinkedHashMap<>());
                String jsonvalue1 = jsonvalue.replaceAll("([=]\\d*| |\\{|\\})", "");
                JSONObject jsonObject = new JSONObject(new LinkedHashMap<>());
                JSONObject jsonObject1 = new JSONObject(new LinkedHashMap<>());
                if (jsonvalue1.length() > 0) {
                    for (String item : jsonvalue1.split(",")) {
                        jsonObject.put(item.split(":")[0], item.split(":")[1]);
                    }
                } else {
                    jsonObject.put("A", null);
                }

                //插入呼入呼出信息
                session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
                session.setTimeoutMillis(60000);
                Upsert upsert11 = table.newUpsert();
                PartialRow row11 = upsert11.getRow();
                row11.addString("sessionid", sessionid);
                row11.addString("demand_id", demand_id);
                row11.addString("mobile", mobile);
                row11.addString("record_url", record_url);
                row11.addString("intention_results", intention_results);
                row11.addString("answer_time", answer_time);
                row11.addString("call_type", call_type);
                row11.addString("isteam", isTeam);
                if (price.replace(" ", "").length() > 0) {
                    row11.addString("isstop", "0");
                } else if ((isStop.replace(" ", "")).length() > 0) {
                    row11.addString("isstop", isStop);
                }
                row11.addString("startcity", startCity);
                row11.addString("endcity", endCity);
                row11.addString("emptydate", emptyDate);
                row11.addString("cartype", carType);
                row11.addString("carlength", carLength);
                row11.addString("goods_name", goods_name);
                row11.addString("price", price);
                if (jsonObject.toJSONString().length() > 3) {
                    row11.addString("non_standard", jsonObject.toJSONString());
                }

                if (price.replace(" ", "").length() > 0) {
                    row11.addInt("is_tempstop", 0);
                } else if (isTempStop > 0) {
                    row11.addInt("is_tempstop", isTempStop);
                }

                String Utc_create_time = create_time;

                TimeZone utc = TimeZone.getTimeZone("UTC");
                if (create_time.contains("T")) {
                    SimpleDateFormat sourceFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                    SimpleDateFormat destFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    sourceFormat.setTimeZone(utc);
                    Date convertedDate = sourceFormat.parse(create_time);
                    Utc_create_time = destFormat.format(convertedDate);
                } else {
                    Utc_create_time = create_time;
                }
                row11.addString("create_time", Utc_create_time);

                session.apply(upsert11);
                session.flush();

                //插入统计汇总表呼入呼出信息
                Upsert upsert12 = table1.newUpsert();
                PartialRow row12 = upsert12.getRow();
                row12.addString("mobile", mobile);
                if ((isTeam.replace(" ", "")).length() > 0) {
                    row12.addString("isteam", isTeam);
                }

                if (price.replace(" ", "").length() > 0) {
                    row12.addString("isstop", "0");
                } else if ((isStop.replace(" ", "")).length() > 0) {
                    row12.addString("isstop", isStop);
                }
                if ((startCity.replace(" ", "")).length() > 0) {
                    row12.addString("startcity", startCity);
                }
                if ((endCity.replace(" ", "")).length() > 0) {
                    row12.addString("endcity", endCity);
                }
                if ((emptyDate.replace(" ", "")).length() > 0) {
                    row12.addString("emptydate", emptyDate);
                }
                if ((carType.replace(" ", "")).length() > 0) {
                    row12.addString("cartype", carType);
                }
                if ((carLength.replace(" ", "")).length() > 0) {
                    row12.addString("carlength", carLength);
                }
                if ((goods_name.replace(" ", "")).length() > 0) {
                    row12.addString("goods_name", goods_name);
                }
                if ((price.replace(" ", "")).length() > 0) {
                    row12.addString("price", price);
                }

                if (price.replace(" ", "").length() > 0) {
                    row12.addInt("is_tempstop", 0);
                } else if (isTempStop > 0) {
                    row12.addInt("is_tempstop", isTempStop);
                }

                String jsonvalue11 = jsonvalue.replaceAll("([=]\\d*| |\\{|\\})", "");
                JSONObject jsonObject11 = new JSONObject(new LinkedHashMap<>());
                //这里先查询
                List<String> projectColumns = new ArrayList<String>();
                //添加select 字段名
                projectColumns.add("non_standard"); //字段名
                KuduScanner.KuduScannerBuilder builder = client.newScannerBuilder(table1).setProjectedColumnNames(projectColumns);
                KuduPredicate predicate1 = null;
                predicate1 = predicate1.newComparisonPredicate(table1.getSchema().getColumn("mobile"),
                        KuduPredicate.ComparisonOp.EQUAL, mobile);
                builder.addPredicate(predicate1);
                KuduScanner scanner = builder.build();
                JSONObject jsonvalue_old = new JSONObject(new LinkedHashMap<>());

                while (scanner.hasMoreRows()) {
                    RowResultIterator results = scanner.nextRows();
                    int numRows = results.getNumRows();
                    System.out.println("numRows count is : " + numRows);
                    while (results.hasNext()) {
                        RowResult result = results.next();
                        if (result.isNull(0)) {
                            break;
                        }
//                            jsonvalue_old = jsonObject;
                        String non_standards = result.getString(0);
                        jsonvalue_old = JSONObject.parseObject(non_standards);
                        if (jsonvalue11.length() > 3) {
                            for (String item : jsonvalue11.split(",")) {
//                                    System.out.println(jsonObject11.getString(item.split(":")[0]));
                                jsonvalue_old.put(item.split(":")[0], item.split(":")[1]);
                            }
                        } else {
                            jsonvalue_old.put("A", null);
                        }
                    }
                }
                System.out.println(jsonvalue_old.toJSONString());

                if ((jsonvalue_old.toJSONString().replace(" ", "")).length() > 3) {
                    row12.addString("non_standard", jsonvalue_old.toJSONString());
                }
                row12.addString("create_time", Utc_create_time);
                logger.info(row12.toString());
                session.apply(upsert12);
                session.flush();

                //需求单信息的写入明细表
                if (call_type.equals("in")) {
                    Upsert upsert22 = table.newUpsert();
                    PartialRow row222 = upsert22.getRow();
                    row222.addString("sessionid", sessionid);
                    row222.addString("demand_id", demand_id);
                    row222.addString("mobile", mobile);
                    row222.addString("call_type", call_type);
                    row222.addString("startcity", empty_city);
                    row222.addString("emptydate", use_car_start);
                    JSONObject jsonObject_line_prefer21 = new JSONObject(new LinkedHashMap<>());
                    if (line_prefer.replace(" ", "").length() > 0) {
                        jsonObject_line_prefer21.put("1", line_prefer);
                    }
                    if (jsonObject_line_prefer21.toJSONString().length() > 3) {
                        row222.addString("line_prefer", jsonObject_line_prefer21.toJSONString());
                    }
                    session.apply(upsert22);
                    session.flush();
                }


                //插入需求单信息到统计汇总表
                JSONObject jsonObject_line = new JSONObject(new LinkedHashMap<>());
                JSONObject jsonvalue_line = new JSONObject(new LinkedHashMap<>());

                String line_prefer1 = "";
                if((null!=value.getField(21)) && (value.getField(21).toString().replace(" ","").length()>0)) {
                    line_prefer1 = value.getField(0).toString();
                }

                JSONObject jsonObject_line12 = new JSONObject(new LinkedHashMap<>());
                JSONObject jsonvalue_line12 = new JSONObject(new LinkedHashMap<>());

                if (call_type.equals("in")) {
                    System.out.println("mobile1:" + mobile);
                    Upsert upsert44 = table1.newUpsert();
                    PartialRow row44 = upsert44.getRow();
                    row44.addString("mobile", mobile);
                    row44.addString("startcity", empty_city);
                    row44.addString("emptydate", use_car_start);
                    JSONObject jsonObject_line_prefer44 = new JSONObject(new LinkedHashMap<>());
                    //查询line_prefer
                    List<String> projectColumns2 = new ArrayList<String>();
                    //添加select 字段名
                    projectColumns2.add("line_prefer"); //字段名
                    KuduScanner.KuduScannerBuilder builder3 = client.newScannerBuilder(table1).setProjectedColumnNames(projectColumns2);
                    KuduPredicate predicate3 = null;
                    predicate3 = predicate3.newComparisonPredicate(table1.getSchema().getColumn("mobile"),
                            KuduPredicate.ComparisonOp.EQUAL, mobile);
                    builder3.addPredicate(predicate3);
                    KuduScanner scanner3 = builder3.build();
                    JSONObject jsonObject_line_prefer3 = new JSONObject(new LinkedHashMap<>());
                    JSONObject jsonObject_line_prefer33 = new JSONObject(new LinkedHashMap<>());

                    while (scanner3.hasMoreRows()) {
                        RowResultIterator results3 = scanner3.nextRows();
                        int numRows3 = results3.getNumRows();
                        System.out.println("numRows count is : " + numRows3);
                        while (results3.hasNext()) {
                            RowResult result3 = results3.next();
                            String get_line_prefer1 = "";
                            if (((result3.isNull(0)) || (result3.getString(0).replace(" ", "").length() < 3)) && (line_prefer.replace(" ", "").length() > 3)) {
                                jsonObject_line_prefer3.put("1", line_prefer);
                                System.out.println("get_line_prefer0" + jsonObject_line_prefer3.toJSONString());
                                row44.addString("line_prefer", jsonObject_line_prefer3.toJSONString());
                            } else if (!(result3.isNull(0)) && (line_prefer.replace(" ", "").length() > 3)) {
                                get_line_prefer1 = result3.getString(0);
                                System.out.println("line_prefer:"+line_prefer);
                                System.out.println("get_line_prefer1" + get_line_prefer1);
                                jsonObject_line_prefer3 = JSONObject.parseObject(get_line_prefer1);
                                jsonObject_line_prefer33.put("1", line_prefer);
                                String xx = get_line_prefer1.replaceAll("([{]|[}]|[\"])", "");
                                System.out.println("xx:" + xx);
                                String[] tmp = xx.split(",");
                                int len = tmp.length;
                                System.out.println(tmp);
                                if (len >= 2) {
//                                        jsonObject_line_prefer33.put(tmp[len - 1].split(":")[0], tmp[len - 1].split(":")[1]);
                                    jsonObject_line_prefer33.put("2", tmp[0].split(":")[1]);
//                                        jsonObject_line_prefer33.put(tmp[len - 2].split(":")[0], tmp[len - 2].split(":")[1]);
                                    jsonObject_line_prefer33.put("3", tmp[1].split(":")[1]);
//                                    }
                                } else if (len < 2) {
                                    System.out.println("tmp0:" + tmp[0]);
                                    jsonObject_line_prefer33.put("2", tmp[0].split(":")[1]);
                                }
                                row44.addString("line_prefer", jsonObject_line_prefer33.toJSONString());
                            }
                            session.apply(upsert44);
                            session.flush();
                        }
                    }
                }
            } catch (Exception e) {
                logger.error(e.getMessage(),e);
                e.printStackTrace();
            }
        }
    }
}
