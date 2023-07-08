package com.cw.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cw.bean.GmallConfig;
import redis.clients.jedis.Jedis;

import java.util.List;

public class DimUtil {

    /**
     * @param table DIM_USER_INFO
     * @param value 17
     * @return 维度信息
     */
    public static JSONObject getDim(String table, String value) {

        //查询Redis
        Jedis jedis = RedisUtil.getJedis();
//        String redisKey = table + ":" + value;
//        String redisKey = "DIM:"+table + ":" + value;
        String redisKey = "GMALL:DIM:"+table + ":" + value;
        String dimJson = jedis.get(redisKey);
        if (dimJson != null && dimJson != "{}" && dimJson.length() > 0) {
//            归还连接
            jedis.close();
//            返回结果
            return JSON.parseObject(dimJson);
        }

        //创建查询的SQL
        String querySql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + table + " where id = '" + value + "'";

        //查询Phoenix
        List<JSONObject> queryList = PhoenixUtil.queryList(querySql, JSONObject.class, false);

        //将从Phoenix查询到的结果写入Redis
        JSONObject jsonObject = queryList.get(0);
        jedis.set(redisKey, jsonObject.toJSONString());
        jedis.expire(redisKey, 24 * 60 * 60);
        jedis.close();

        //返回结果数据
        return jsonObject;
    }

    public static void deleteCache(String key) {
        Jedis jedis = RedisUtil.getJedis();
        jedis.del(key);
        jedis.close();
    }

    public static void main(String[] args) {

        long first = System.currentTimeMillis();
        System.out.println(getDim("DIM_USER_INFO", "2"));
        long second = System.currentTimeMillis();
        System.out.println(getDim("DIM_USER_INFO", "2"));
        long third = System.currentTimeMillis();
        System.out.println(getDim("DIM_USER_INFO", "2"));
        long third2 = System.currentTimeMillis();
        System.out.println(second - first);
        System.out.println(third - second);
        System.out.println(third2 - third);
    }

}
