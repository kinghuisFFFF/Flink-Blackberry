package com.cw.utils;

import com.alibaba.fastjson.JSONObject;
import com.cw.bean.GmallConfig;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PhoenixUtil {

    private static Connection connection;

    private static Connection getConnection() {
        try {
            Class.forName(GmallConfig.PHOENIX_DRIVER);
            return DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("获取连接失败！");
        }
    }

    public static <T> List<JSONObject> queryList(String sql, Class<T> clz, Boolean underScoreToCamel) {

        //1.获取连接
        if (connection == null) {
            connection = getConnection();
        }

        PreparedStatement preparedStatement = null;

        ResultSet resultSet = null;
        ArrayList<T> resultList = new ArrayList<>();
        ArrayList<JSONObject> resultList2 = new ArrayList<>();

        try {
            //2.编译SQL
            preparedStatement = connection.prepareStatement(sql);

            //3.执行查询
            resultSet = preparedStatement.executeQuery();

            //4.处理resultSet结果集
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (resultSet.next()) {

                //创建泛型对象
                T t = clz.newInstance();
                JSONObject t1 = (JSONObject) clz.newInstance();

                for (int i = 1; i < columnCount + 1; i++) {

                    //获取列名
                    String columnName = metaData.getColumnName(i);
                    if (underScoreToCamel) {
                        columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                    }

                    //获取列值
                    String value = resultSet.getString(columnName);

                    //给泛型对象赋值
                    BeanUtils.setProperty(t, columnName, value);
                    t1.put(columnName,value);
                }

                //将泛型对象加入集合
//                resultList.add(t);
                resultList2.add(t1);
            }


        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("查询维度" + sql + "信息失败");
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        return resultList2;

    }


    public static <T> List<T> queryList2(String sql, Class<T> clz, Boolean underScoreToCamel) {

        //1.获取连接
        if (connection == null) {
            connection = getConnection();
        }

        PreparedStatement preparedStatement = null;

        ResultSet resultSet = null;
        ArrayList<T> resultList = new ArrayList<>();

        try {
            //2.编译SQL
            preparedStatement = connection.prepareStatement(sql);

            //3.执行查询
            resultSet = preparedStatement.executeQuery();

            //4.处理resultSet结果集
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (resultSet.next()) {

                //创建泛型对象
                T t = clz.newInstance();
                JSONObject t1 = (JSONObject) clz.newInstance();

                for (int i = 1; i < columnCount + 1; i++) {

                    //获取列名
                    String columnName = metaData.getColumnName(i);
                    if (underScoreToCamel) {
                        columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                    }

                    //获取列值
                    String value = resultSet.getString(columnName);

                    //给泛型对象赋值
                    BeanUtils.setProperty(t, columnName, value);
                    t1.put(columnName,value);
                }

                //将泛型对象加入集合
                resultList.add(t);
            }


        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("查询维度" + sql + "信息失败");
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        return resultList;

    }

    public static void main(String[] args) {

//        List<JSONObject> queryList = queryList("select * from GMALL200923_REALTIME.DIM_USER_INFO", JSONObject.class, false);

        //查询Phoenix
//        String querySql="select * from \"ns1\".DIM_USER_INFO where id = '2'";
        String querySql="select * from ods.\"test1\"";
        List<JSONObject> queryList2 = PhoenixUtil.queryList(querySql, JSONObject.class, false);
//        List<JSONObject> queryList2 = PhoenixUtil.queryList(querySql, JSONObject.class, true); // 第3个参数，true开启驼峰命名
//        List<JSONObject> queryList2 = PhoenixUtil.queryList2(querySql, JSONObject.class, false);

        for (JSONObject jsonObject : queryList2) {
            System.out.println(jsonObject);
        }

    }

}
