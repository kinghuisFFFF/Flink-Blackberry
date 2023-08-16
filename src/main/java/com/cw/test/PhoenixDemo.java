package com.cw.test;

import com.alibaba.fastjson.JSONObject;
import com.cw.utils.PhoenixUtil;

import java.util.List;

/**
 * @Title:
 * @BelongProjecet Flink-Blackberry
 * @BelongPackage com.cw.test
 * @Description:
 * @Copyright time company - Powered By 研发一部
 * @Author: cw
 * @Date: 2023/7/17 10:56
 * @Version V1.0
 */
public class PhoenixDemo {
    public static void main(String[] args) {
        String querySql="select * from ods.\"test1\"";
        List<JSONObject> queryList2 = PhoenixUtil.queryList(querySql, JSONObject.class, false);
//        List<JSONObject> queryList2 = PhoenixUtil.queryList(querySql, JSONObject.class, true); // 第3个参数，true开启驼峰命名
//        List<JSONObject> queryList2 = PhoenixUtil.queryList2(querySql, JSONObject.class, false);

        for (JSONObject jsonObject : queryList2) {
            System.out.println(jsonObject);
        }
    }
}
