package com.cw.test;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class ClientDemo3 {
    /**
     * 发送端
     */
    public static void main(String[] args) throws Exception {
        System.out.println("=============发送端启动===========");
        // 1.创建发送端对象
        DatagramSocket socket = new DatagramSocket(6666);

        // 2.创建一个数据包对象封装数据
        /**
         * 参数1:封装要发送的数据
         * 参数2:发送数据的大小
         * 参数3:服务端的IP地址
         * 参数4:服务端的端口
         *
         * InetAddress.getLocalHost() 获取本机IP地址
         */
        Map map = new HashMap();
        Random r = new Random();
        int min = 2; // 定义随机数的最小值
        int max = 102; // 定义随机数的最大值

//float f1 = r.nextFloat(); // 生成一个随机浮点型值
//float f1 = r.nextFloat()+100; // 生成一个随机浮点型值
        while (true){
            for (int i = 0; i < 3100; i++) {
                // 产生一个2~100的数
                 // 生成一个随机浮点型值
                float f1 = (float) min + (float) (Math.random() * (max - min));
                map.put("a-"+i, f1);
            }

//        byte[] buffer = "我是一坨数据!".getBytes();
            byte[] buffer = map.toString().getBytes();
            DatagramPacket packet = new DatagramPacket(buffer,buffer.length, InetAddress.getLocalHost(),8888);
//            DatagramPacket packet = new DatagramPacket(buffer,buffer.length,InetAddress.getByName("192.168.3.237"),5005);

            // 3.发送数据
            socket.send(packet);
        }

        // 4.关闭管道
//        socket.close();
    }
}
