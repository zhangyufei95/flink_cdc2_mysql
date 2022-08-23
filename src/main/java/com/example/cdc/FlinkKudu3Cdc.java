package com.example.cdc;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kudu.client.*;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Logger;

public class FlinkKudu3Cdc {
    private static String name = FlinkKudu3Cdc.class.getName();
    private static Logger log = Logger.getLogger(name);// <= (2)

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(FlinkKudu3Cdc.class);
    //master地址
    private static String masteraddr = "172.27.16.41:7051,172.27.16.43:7051,172.27.16.49:7051";
    private static String kuduTableName = "stage.stage_dwd_driver_portrait_profile";


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        /**
         * 故障恢复ck的设置
         */
        //ck 不做ck 每次都会从最新的位置开始读取数据
//        env.enableCheckpointing(5000L); //每5秒做一次ck
        //ck 类型：EXACTLY_ONCE 精准一次性 另一种是 AT_LEAST_ONCE 最少一次
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置存储的状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://linux1:9820/flink/CDC/ck"));
        //设置重启策略 相当于无限重启
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 1000L));
        //访问hdfs，设置访问HDFS的用户名
//        System.setProperty("HADOOP_USER_NAME", "root");
        //如果从页面cancal的任务，是会删除ck的，但是取消的任务也是需要保留ck的，加一个参数
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        /**
         * flink cdc初始化的方式 :scan.startup.mode
         *  1、initial (default): Performs an initial snapshot on the monitored database tables upon first startup, and continue to read the latest binlog.
         *  历史数据利用查询的方式读取，读完历史的紧接着切换成读取binlog
         *  2、latest-offset: Never to perform snapshot on the monitored database tables upon first startup, just read from the end of the binlog which means only have the changes since the connector was started.
         *  从最新的binlog开始读
         *  3、timestamp: Never to perform snapshot on the monitored database tables upon first startup, and directly read binlog from the specified timestamp. The consumer will traverse the binlog from the beginning and ignore change events whose timestamp is smaller than the specified timestamp.
         *  binlog中有一个时间戳，可以从指定的时间开始去读
         *  4、specific-offset: Never to perform snapshot on the monitored database tables upon first startup, and directly read binlog from the specified offset.
         *  从指定的offset开始读
         */
        //创建mysql cdc的source
        Properties properties = new Properties();
        //scan.startup.mode","initial 每次程序重启都要重新读一遍数据库，太费时且可能会产生重复数据，做ck后可解决此问题
        long nowTime =System.currentTimeMillis();
        long todayStartTime =nowTime - ((nowTime + TimeZone.getDefault().getRawOffset()) % (24 * 60 * 60 * 1000L));
        properties.setProperty("scan.startup.mode","latest-offset"); //从当天0点开始（2021-02-21 00:00:00）
        properties.setProperty("debezium.snapshot.locking.mode","none");

        DateFormat format1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String nowDate = format1.format(new Date()).substring(0,10);

        DebeziumSourceFunction<String> mysqlSource_task = MySqlSource.<String>builder().serverId(1609)
                .hostname("172.27.0.48")
                .port(3306)
                .username("flink_cdc_ad")
                .password("jklsek52@=9H")
                .databaseList("ht_user")
                //注意必须是db.table的方式，只写table读取不成功
                //可选配置项,如果不指定该参数,则会读取上一个配置下的所有表的数据
                .tableList("ht_user.wx_msg_task")
                .startupOptions(StartupOptions.latest())
                //flink cdc内置使用了debezium 故可以配置它的一些属性信息，包括CDC读取的初始化方式
//                .debeziumProperties(properties)
                //可自定义读取数据的类型 StringDebeziumDeserializationSchema 因为这里是返回的string，另一种提供的是RowDataDebeziumDeserializeSchema
                .deserializer(new CustomDebeziumDeserializer())
                .build();

        /*
         *  .startupOptions(StartupOptions.latest()) 参数配置
         *  1.initial() 全量扫描并且继续读取最新的binlog 最佳实践是第一次使用这个
         *  2.earliest() 从binlog的开头开始读取 就是啥时候开的binlog就从啥时候读
         *  3.latest() 从最新的binlog开始读取
         *  4.specificOffset(String specificOffsetFile, int specificOffsetPos) 指定offset读取
         *  5.timestamp(long startupTimestampMillis) 指定时间戳读取
         */

        DebeziumSourceFunction<String> mysqlSource_message = MySqlSource.<String>builder().serverId(1610)
                .hostname("172.27.0.48")
                .port(3306)
                .username("flink_cdc_ad")
                .password("jklsek52@=9H")
                .databaseList("ht_user")
                //注意必须是db.table的方式，只写table读取不成功
                //可选配置项,如果不指定该参数,则会读取上一个配置下的所有表的数据
                .tableList("ht_user.wx_message")
                .startupOptions(StartupOptions.latest())
                //flink cdc内置使用了debezium 故可以配置它的一些属性信息，包括CDC读取的初始化方式
                //.debeziumProperties(properties)
                //可自定义读取数据的类型 StringDebeziumDeserializationSchema 因为这里是返回的string，另一种提供的是RowDataDebeziumDeserializeSchema
                .deserializer(new CustomDebeziumDeserializer())
                .build();

        //读取mysql数据
        DataStreamSource<String> stringDS_task = env.addSource(mysqlSource_task);
        DataStreamSource<String> stringDS_message = env.addSource(mysqlSource_message);

        //设置水印时间
        DataStream<String> stringDS_task1 = stringDS_task.assignTimestampsAndWatermarks(new MyExtractor());
        DataStream<String> stringDS_message1 = stringDS_message.assignTimestampsAndWatermarks(new MyExtractor());

        //wx_message 选取字段+过滤当天数据
        Calendar cal = Calendar.getInstance();
        DataStream<String> resultData_message = stringDS_message1.map(data -> {
            JSONObject jsonObj = new JSONObject();
            Date date1 = JSONObject.parseObject(data).getTimestamp("create_time");
//            cal.setTime(date1);
//            cal.add(Calendar.HOUR, 8);// 24小时制
//            date1 = cal.getTime();
            jsonObj.put("create_time",format1.format(date1));
            jsonObj.put("direction",JSONObject.parseObject(data).getString("direction"));
            jsonObj.put("platform_msg_type",JSONObject.parseObject(data).getString("platform_msg_type"));
            jsonObj.put("puid",JSONObject.parseObject(data).getString("platform_u_id"));
            jsonObj.put("ppuid",JSONObject.parseObject(data).getString("profile_platform_u_id"));
            jsonObj.put("text_content",JSONObject.parseObject(data).getString("text_content"));
            jsonObj.put("session_type",JSONObject.parseObject(data).getString("session_type"));
//            jsonObj.put("client_id",JSONObject.parseObject(data).getString("client_id"));
            Date date2 = JSONObject.parseObject(data).getTimestamp("send_date");
            jsonObj.put("send_date",format1.format(date2));
            //时间处理
            return jsonObj.toJSONString();
        }).filter(data -> {
            if ((nowDate.equals(JSONObject.parseObject(data).getString("create_time").substring(0, 10)))
                    && ("1".equals(JSONObject.parseObject(data).getString("session_type")))
                    && !("1011".equals(JSONObject.parseObject(data).getString("session_type"))) && !(JSONObject.parseObject(data).getString("text_content")==null) && !(JSONObject.parseObject(data).getString("text_content").contains("可以开始聊天")))
            { return true; } else { return false; }
        });


        //wx_msg_task 选取字段+当天数据过滤
        DataStream<String> resultData_task = stringDS_task1.map(data -> {
            JSONObject jsonObj = new JSONObject();
            Date date1 = JSONObject.parseObject(data).getTimestamp("create_time");
//            cal.setTime(date1);
//            cal.add(Calendar.HOUR, 8);// 24小时制
//            date1 = cal.getTime();
            jsonObj.put("create_time",format1.format(date1));
            jsonObj.put("puid",JSONObject.parseObject(data).getString("wl_id"));
            jsonObj.put("ppuid",JSONObject.parseObject(data).getString("from_id"));
            jsonObj.put("type",JSONObject.parseObject(data).getString("type"));
            jsonObj.put("demand_id",JSONObject.parseObject(data).getString("demand_id"));
            return jsonObj.toJSONString();
        }).filter(data -> {
            if ((nowDate.equals(JSONObject.parseObject(data).getString("create_time").substring(0,10))) && (!(JSONObject.parseObject(data).getString("type")==null) && ("1,10,3,4".contains(JSONObject.parseObject(data).getString("type")))))
            { return true; } else { return false; }
        });
        //task表首先插入
        resultData_task.addSink(new FlinkKudu3Cdc.KuduSink());
        //message表后插入
        resultData_message.addSink(new FlinkKudu3Cdc.KuduSinkMessage());
        env.execute();
    }

    public static class MyExtractor extends BoundedOutOfOrdernessTimestampExtractor<String> {

        //乱序容忍1s
        public MyExtractor() {
            super(Time.seconds(1));
        }

        @Override
        public long extractTimestamp(String event) {
            return JSONObject.parseObject(event).getTimestamp("create_time").getTime();
        }
    }

    public static void KuduUpdate(KuduClient client, KuduTable table,RowResultIterator results,String message_puid,String message_ppuid,int message_direction,String message_send_date) throws KuduException {
        KuduSession session = client.newSession();
//        table = client.openTable(kuduTableName);
        session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        while (results.hasNext()) {
            RowResult result = results.next();
            String message_demand_id = result.getString(0);
            String message_create_time = result.getString(1);
            String message_type = result.getString(2);
            System.out.println(result.getString(0) + ";" + result.getString(1));
            Update update = table.newUpdate();
            PartialRow row1 = update.getRow();
            row1.addString("puid", message_puid);
            row1.addString("ppuid", message_ppuid);
            row1.addString("demand_id", message_demand_id);
            row1.addString("create_time", message_create_time);
            row1.addString("send_date", message_send_date);
            row1.addString("type", message_type);
            row1.addInt("is_currnet_session", 0);
            if (message_direction == 1) {
                row1.addInt("is_response", 1);
                row1.addInt("direction", 1);
            } else {
                row1.addInt("direction", message_direction);
            }
            System.out.println(row1);
            session.apply(update);
            session.flush();
            session.close();
        }
    }

    public static class KuduSink extends RichSinkFunction<String> {

        /**
         * 数据输出时执行，每一个数据输出时，都会执行此方法
         *
         * @param value
         * @param context
         * @throws Exception
         */
        @Override
        public void invoke(String value, Context context) throws Exception {
            try {

                List<String> projectColumns = new ArrayList<String>();
                //创建kudu的数据库链接
                KuduClient client = new KuduClient.KuduClientBuilder(masteraddr).defaultSocketReadTimeoutMs(6000).build();
                //添加select 字段名
                projectColumns.add("create_time"); //字段名
                //打开表
                KuduTable table = client.openTable(kuduTableName);
                // 简单的读取 newScannerBuilder(查询表)  setProjectedColumnNames(指定输出列)  build()开始扫描
//            KuduScanner scanner = client.newScannerBuilder(table).setProjectedColumnNames(projectColumns).build();
                KuduScanner.KuduScannerBuilder builder = client.newScannerBuilder(table).setProjectedColumnNames(projectColumns);
                /**
                 * 设置搜索的条件 where 条件过滤字段名
                 * 如果不设置，则全表扫描
                 */
                //下面的条件过滤 where user_id = xxx and day = xxx;
                String message_puid = JSONObject.parseObject(value).getString("puid");
                String message_ppuid = JSONObject.parseObject(value).getString("ppuid");
                String message_demand_id = JSONObject.parseObject(value).getString("demand_id");
                String message_type = JSONObject.parseObject(value).getString("type");
                String message_create_time = JSONObject.parseObject(value).getString("create_time");

                //比较方法ComparisonOp：GREATER、GREATER_EQUAL、EQUAL、LESS、LESS_EQUAL
                KuduPredicate predicate1 = null;
                predicate1 = predicate1.newComparisonPredicate(table.getSchema().getColumn("puid"),
                        KuduPredicate.ComparisonOp.EQUAL, message_puid);
                KuduPredicate predicate2 = null;
                predicate2 = predicate2.newComparisonPredicate(table.getSchema().getColumn("ppuid"),
                        KuduPredicate.ComparisonOp.EQUAL, message_ppuid);
                KuduPredicate predicate3 = null;
                predicate3 = predicate3.newComparisonPredicate(table.getSchema().getColumn("demand_id"),
                        KuduPredicate.ComparisonOp.EQUAL, message_demand_id);

                builder.addPredicate(predicate1);
                builder.addPredicate(predicate2);
                builder.addPredicate(predicate3);

                KuduScanner scanner = builder.build();
                KuduSession session = client.newSession();
                session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
                session.setMutationBufferSpace(3000);
                while (scanner.hasMoreRows()) {
                    RowResultIterator results = scanner.nextRows();
//                    KuduUpdate(client,table,results,message_puid,message_ppuid,message_direction,message_send_date);
                    while (results.hasNext()) {
//                        KuduSession session = client.newSession();
                        RowResult result = results.next();
                        String message_create_time1 = result.getString(0);
                        Update update = table.newUpdate();
                        PartialRow row1 = update.getRow();
                        row1.addString("puid",message_puid);
                        row1.addString("ppuid",message_ppuid);
                        row1.addString("demand_id",message_demand_id);
                        row1.addString("create_time",message_create_time1);
                        row1.addString("type",message_type);
                        row1.addInt("is_currnet_session",0);
                        System.out.println("运营号再次推送消息 ---> message_puid: "+message_puid+"; message_ppuid: "+message_ppuid+"; message_demand_id: "+message_demand_id+" ; message_type: "+message_type);
                        session.apply(update);
                        session.flush();
                    }
                }
//                session.setTimeoutMillis(60000);
                Insert insert = table.newInsert();
                PartialRow row = insert.getRow();
                row.addString("puid",message_puid);
                row.addString("ppuid",message_ppuid);
                row.addString("demand_id",message_demand_id);
                row.addString("create_time",message_create_time);
                row.addString("type",message_type);
                row.addInt("is_currnet_session",1);
                row.addInt("is_response", 0);
                row.addInt("direction",2);
                System.out.println("运营号新推送消息 ---> message_puid: "+message_puid+"; message_ppuid: "+message_ppuid+"; message_demand_id: "+message_demand_id+" ; message_type: "+message_type);
                session.apply(insert);
                session.close();
                client.close();
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Wrong!");
            }
            System.out.println("Done Successfully!");
        }
    }

    public static class KuduSinkMessage extends RichSinkFunction<String> {
        /**
         * 数据输出时执行，每一个数据输出时，都会执行此方法
         *
         * @param value
         * @param context
         * @throws Exception
         */
        @Override
        public void invoke(String value, Context context) throws Exception {
            try {
                List<String> projectColumns = new ArrayList<String>();
                //创建kudu的数据库链接
                KuduClient client = new KuduClient.KuduClientBuilder(masteraddr).defaultSocketReadTimeoutMs(6000).build();
                //添加select 字段名
                projectColumns.add("demand_id"); //字段名
                projectColumns.add("create_time"); //字段名
                projectColumns.add("type"); //字段名
                projectColumns.add("is_response"); //字段名

                //打开表
                KuduTable table = client.openTable(kuduTableName);
                // 简单的读取 newScannerBuilder(查询表)  setProjectedColumnNames(指定输出列)  build()开始扫描
//            KuduScanner scanner = client.newScannerBuilder(table).setProjectedColumnNames(projectColumns).build();
                KuduScanner.KuduScannerBuilder builder = client.newScannerBuilder(table).setProjectedColumnNames(projectColumns);
                /**
                 * 设置搜索的条件 where 条件过滤字段名
                 * 如果不设置，则全表扫描
                 */
                //下面的条件过滤 where user_id = xxx and day = xxx;
                String message_puid = JSONObject.parseObject(value).getString("puid");
                String message_ppuid = JSONObject.parseObject(value).getString("ppuid");
                int message_direction = JSONObject.parseObject(value).getInteger("direction");
                String message_send_date = JSONObject.parseObject(value).getString("send_date");

                //比较方法ComparisonOp：GREATER、GREATER_EQUAL、EQUAL、LESS、LESS_EQUAL
                KuduPredicate predicate1 = null;
                predicate1 = predicate1.newComparisonPredicate(table.getSchema().getColumn("puid"),
                        KuduPredicate.ComparisonOp.EQUAL, message_puid);
                KuduPredicate predicate2 = null;
                predicate2 = predicate2.newComparisonPredicate(table.getSchema().getColumn("ppuid"),
                        KuduPredicate.ComparisonOp.EQUAL, message_ppuid);

                builder.addPredicate(predicate1);
                builder.addPredicate(predicate2);

                KuduScanner scanner = builder.build();
                int numRows = 0;
//              KuduTable table = client.openTable(kuduTableName);
                KuduSession session = client.newSession();
                session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);

                session.setMutationBufferSpace(3000);
                while (scanner.hasMoreRows()) {
                    RowResultIterator results = scanner.nextRows();
//                    KuduUpdate(client,table,results,message_puid,message_ppuid,message_direction,message_send_date);
                    while (results.hasNext()) {
//                        KuduSession session = client.newSession();
                        RowResult result = results.next();
                        String message_demand_id = result.getString(0);
                        String message_create_time = result.getString(1);
                        String message_type = result.getString(2);
                        int message_is_response = result.getInt(3);

                        Update update = table.newUpdate();
                        PartialRow row1 = update.getRow();
                        row1.addString("puid",message_puid);
                        row1.addString("ppuid",message_ppuid);
                        row1.addString("demand_id",message_demand_id);
                        row1.addString("create_time",message_create_time);
                        row1.addString("type",message_type);
                        row1.addInt("is_currnet_session",1);
                        if(message_direction == 1) {
                            System.out.println("司机回复消息 ---> message_puid: "+message_puid+"; message_ppuid: "+message_ppuid+"; message_direction: "+message_direction+" ; message_type: "+message_type);
                            row1.addInt("is_response", message_is_response+1);
                            row1.addInt("direction",1);
                            row1.addString("send_date",message_send_date);
                            session.apply(update);
                            session.flush();
                        }
                    }
                }
//                session.setTimeoutMillis(60000);
                if(message_direction == 1) {
                    Insert insert = table.newInsert();
                    PartialRow row = insert.getRow();
                    row.addString("puid", message_puid);
                    row.addString("ppuid", message_ppuid);
                    row.addString("demand_id", "1");
                    row.addString("create_time", "1");
                    row.addString("type", "0");
                    row.addString("send_date", message_send_date);
                    row.addInt("is_currnet_session", 1);
                    row.addInt("is_response", 0);
                    row.addInt("direction", message_direction);
                    System.out.println("司机主动问询消息 ---> message_puid: " + message_puid + "; message_ppuid: " + message_ppuid + "; message_direction: " + message_direction + " ; send_date: " + message_send_date);
                    session.apply(insert);
                }
                session.close();
                client.close();
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Wrong!");
            }
            System.out.println("Done Successfully!");
        }
    }
}

