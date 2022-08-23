package com.example.cdc;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class UrlPost {

    public static void main(String[] args) {
        String url = "https://gw.heptax.com/algo/recommend/get_city_belong";
        JSONObject xx = new JSONObject();
//        xx.put("city_slot_list",["上海-南京"]);
//        String data = {"city_slot_list":txt}
//        Map<String,String> map1 = new HashMap<>();
//        map1.put("Content-Type","application/json; charset=UTF-8");
//        map1.put("User-Agent","Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Mobile Safari/537.36");
//        map1.put("Accept","application/json, text/javascript, */*; q=0.01");
//        System.out.println(sendPost(url,));
    }

    /**
     * 向指定URL发送GET方法的请求
     */
//    public static String sendGet(String url, String param) throws UnsupportedEncodingException, IOException {
//        return sendGet(url, param, null);
//    }
//    public static String sendGet(String url, String param, Map<String, String> header) throws UnsupportedEncodingException, IOException {
//        String result = "";
//        BufferedReader in = null;
//        String urlNameString = url + "?" + param;
//        URL realUrl = new URL(urlNameString);
//        // 打开和URL之间的连接
//        URLConnection connection = realUrl.openConnection();
//        //设置超时时间
//        connection.setConnectTimeout(5000);
//        connection.setReadTimeout(15000);
//        // 设置通用的请求属性
//        if (header!=null) {
//            Iterator<Entry<String, String>> it =header.entrySet().iterator();
//            while(it.hasNext()){
//                Entry<String, String> entry = it.next();
//                System.out.println(entry.getKey()+":::"+entry.getValue());
//                connection.setRequestProperty(entry.getKey(), entry.getValue());
//            }
//        }
//        connection.setRequestProperty("accept", "*/*");
//        connection.setRequestProperty("connection", "Keep-Alive");
//        connection.setRequestProperty("user-agent","Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
//        // 建立实际的连接
//        connection.connect();
//        // 获取所有响应头字段
//        Map<String, List<String>> map = connection.getHeaderFields();
//        // 遍历所有的响应头字段
//        for (String key : map.keySet()) {
//            System.out.println(key + "--->" + map.get(key));
//        }
//        // 定义 BufferedReader输入流来读取URL的响应，设置utf8防止中文乱码
//        in = new BufferedReader(new InputStreamReader(connection.getInputStream(), "utf-8"));
//        String line;
//        while ((line = in.readLine()) != null) {
//            result += line;
//        }
//        if (in != null) {
//            in.close();
//        }
//        return result;
//    }

    /**
     * 向指定 URL 发送POST方法的请求
     */
    public static String sendPost(String url, String param) throws UnsupportedEncodingException, IOException {
        return sendPost(url, param, null);
    }

    public static String sendPost(String url, String param, Map<String, String> header) throws UnsupportedEncodingException, IOException {
        PrintWriter out = null;
        BufferedReader in = null;
        String result = "";
        URL realUrl = new URL(url);
        // 打开和URL之间的连接
        URLConnection conn = realUrl.openConnection();
        //设置超时时间
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(15000);
        // 设置通用的请求属性
        if (header!=null) {
            for (Entry<String, String> entry : header.entrySet()) {
                conn.setRequestProperty(entry.getKey(), entry.getValue());
            }
        }
        conn.setRequestProperty("accept", "*/*");
        conn.setRequestProperty("connection", "Keep-Alive");
        conn.setRequestProperty("user-agent",
                "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
        // 发送POST请求必须设置如下两行
        conn.setDoOutput(true);
        conn.setDoInput(true);
        // 获取URLConnection对象对应的输出流
        out = new PrintWriter(conn.getOutputStream());
        // 发送请求参数
        out.print(param);
        // flush输出流的缓冲
        out.flush();
        // 定义BufferedReader输入流来读取URL的响应
        in = new BufferedReader(
                new InputStreamReader(conn.getInputStream(), "utf8"));
        String line;
        while ((line = in.readLine()) != null) {
            result += line;
        }
        if(out!=null){
            out.close();
        }
        if(in!=null){
            in.close();
        }
        return result;
    }
}

