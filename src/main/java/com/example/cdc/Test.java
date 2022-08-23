package com.example.cdc;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.apache.kudu.client.*;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;


import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.alibaba.fastjson.asm.Type.getType;
import static java.sql.JDBCType.NULL;

public class Test {

    private static List<String> projectColumns = new ArrayList<String>();
    private static KuduTable table;
    private static KuduScanner.KuduScannerBuilder builder;
    private static KuduScanner scanner;
    private static KuduPredicate predicate1;
    private static KuduPredicate predicate2;


    public static void main(String[] args) throws IOException, ParseException {
        String url = "https://gw.heptax.com/algo/recommend/user_similar_recomm_init";
        URL serverUrl = new URL(url);
        HttpURLConnection conn = (HttpURLConnection) serverUrl.openConnection();
        // 设置是否向connection输出，因为这个是post请求，参数要放在
        // http正文内，因此需要设为true
        conn.setDoOutput(Boolean.TRUE);
        conn.setDoInput(Boolean.TRUE);
        //请求方式是POST
        conn.setRequestMethod("POST");
        // Post 请求不能使用缓存
        conn.setUseCaches(false);
        conn.setRequestProperty("Content-type", "application/json");
        //必须设置false，否则会自动redirect到重定向后的地址
        conn.setInstanceFollowRedirects(false);
        //建立连接
        conn.connect();
        //设置请求体
        HashMap map=new HashMap();
        //key-value的形式设置请求参数
        map.put("mode","short");
        String s = JSONObject.toJSONString(map);
        //获取了返回值
        String result = HttpsUtils.getReturn(conn,s);
        //如果返回值是标准的JSON字符串可以像我这样给他进行转换
        JSONObject jsonObject = JSONObject.parseObject(result);
        System.out.println(jsonObject);
        System.exit(0);


        TimeZone utc = TimeZone.getTimeZone("UTC");

        SimpleDateFormat sourceFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

        SimpleDateFormat destFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        sourceFormat.setTimeZone(utc);

        Date convertedDate = sourceFormat.parse("2018-05-23T23:22:16.000Z");

        System.out.println(destFormat.format(convertedDate));
        System.exit(0);



        String json_str = "{\"ns_001\":\"需要盖雨布\",\"ns_002\":\"需要上叉车\",\"ns_003\":\"需要司机帮装\",\"ns_017\":\"需要排队\",\"ns_004\":\"需等配货\",\"ns_018\":\"驾驶员不准下车\",\"ns_005\":\"底板要平整\",\"ns_006\":\"需要枕木\",\"ns_007\":\"需要垫薄膜\",\"ns_019\":\"要有灭火器\",\"ns_008\":\"高栏全拆立柱\",\"ns_009\":\"高栏半拆立柱\",\"ns_010\":\"国五车及以上\",\"ns_011\":\"全程走高速\",\"ns_020\":\"装货地禁区\",\"ns_021\":\"卸货地禁区\",\"ns_022\":\"需48小时核酸\",\"ns_023\":\"需24小时核酸\",\"ns_024\":\"需通行证\"}";
        JSONObject jsonObject2 = new JSONObject();
        jsonObject2.put("A","1");
        jsonObject2.put("B","1");
        jsonObject2.put("C","1");
        jsonObject2.put("A","2");
        jsonObject2.put("D",null);

        System.out.println(jsonObject2);
        System.exit(0);

        String time1 = "2022-03-03 12:09:00";
        String time2 = "2022-03-03 10:00:00";
        System.out.println(time1.compareTo(time2));
        System.exit(0);

        System.out.println("10,3".contains("3"));
        System.exit(0);

        long nowTime =System.currentTimeMillis();
        long todayStartTime =nowTime - ((nowTime + TimeZone.getDefault().getRawOffset()) % (24 * 60 * 60 * 1000L));
        System.out.println(todayStartTime);
        System.exit(0);

        JSONObject jsonObj = new JSONObject();
        jsonObj.put("type","");
        String js = jsonObj.toJSONString();

        System.out.println(JSONObject.parseObject(js).getString("type")=="");
        System.exit(0);
        System.out.println("1,10,3,4".contains("14"));

        System.exit(0);
        Date d = new Date();

        DateFormat format1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(format1.format(d.getTime()));
        String nowDate = format1.format(new Date()).substring(0,10);

        System.out.println(nowDate);

        //本方法是插入数据库之前先查询，实现upsert
        final String masteraddr = "172.27.16.41:7051,172.27.16.43:7051,172.27.16.49:7051";
        try {
            //创建kudu的数据库链接
            KuduClient client = new KuduClient.KuduClientBuilder(masteraddr).defaultSocketReadTimeoutMs(6000).build();

            projectColumns.add("userid_first_send_time"); //字段名
            projectColumns.add("puid_first_send_time");
            table = client.openTable("stage.stage_driver_portrait_profile_df");

            // 简单的读取 newScannerBuilder(查询表)  setProjectedColumnNames(指定输出列)  build()开始扫描
//            KuduScanner scanner = client.newScannerBuilder(table).setProjectedColumnNames(projectColumns).build();

            builder = client.newScannerBuilder(table).setProjectedColumnNames(projectColumns);
            /**
             * 设置搜索的条件 where 条件过滤字段名
             * 如果不设置，则全表扫描
             */
            //下面的条件过滤 where user_id = xxx and day = xxx;
//            String p_u_id = value.getField(0).toString();
//            String user_id = value.getField(1).toString();
//            long userID = 7232560922086310458L;
//            int Day = 17889;
            //比较方法ComparisonOp：GREATER、GREATER_EQUAL、EQUAL、LESS、LESS_EQUAL
            predicate1 = predicate1.newComparisonPredicate(table.getSchema().getColumn("p_u_id"),
                    KuduPredicate.ComparisonOp.LESS_EQUAL, "101");
            predicate2 = predicate2.newComparisonPredicate(table.getSchema().getColumn("user_id"),
                    KuduPredicate.ComparisonOp.LESS_EQUAL, "202");

            builder.addPredicate(predicate1);
            builder.addPredicate(predicate2);

            // 开始扫描
            scanner = builder.build();


            while (scanner.hasMoreRows()) {
                RowResultIterator results = scanner.nextRows();
                int numRows = results.getNumRows();
                if(numRows > 0){
                    break;
                }
            }
                /*
                  RowResultIterator.getNumRows()
                        获取此迭代器中的行数。如果您只想计算行数，那么调用这个函数并跳过其余的。
                        返回：此迭代器中的行数
                        如果查询出数据则 RowResultIterator.getNumRows() 返回的是查询数据的行数，如果查询不出数据返回0
                 */
                // 每次从tablet中获取的数据的行数
//                int numRows = results.getNumRows();

                //若是仅仅判断是否存在numRows>0即可
//                System.out.println("numRows count is : " + numRows);
//                while (results.hasNext()) {
//                    RowResult result = results.next();
//                    String userid_first_send_time = result.getString(0);
//                    String  puid_first_send_time = result.getString(1);
//                    System.out.println("userid_first_send_time is : " + userid_first_send_time + "  ===  puid_first_send_time: " + puid_first_send_time);
//                }
//                System.out.println("--------------------------------------");
//            }?
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Wrong!");
        }
        System.out.println("Done Successfully!");
        }
    }
