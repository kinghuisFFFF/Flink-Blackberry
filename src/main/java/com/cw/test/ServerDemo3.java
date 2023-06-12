package com.cw.test;

import java.net.DatagramPacket;
import java.net.DatagramSocket;

import static com.cw.test.WaveDataCalcUtil.bytes2Float;

/**
 * 接收端
 */
public class ServerDemo3 {
    public static void main(String[] args) throws Exception {
        System.out.println("=============客户端启动===========");

        // 1.创建接受对象
        DatagramSocket socket = new DatagramSocket(8888);
//        DatagramSocket socket = new DatagramSocket(3333);

        // 2.创建一个数据包接收数据
        byte [] buffer = new byte[56716+1024];
//        byte [] buffer = new byte[12000];
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

        while (true){
            // 3.等待接受数据
            socket.receive(packet);

            // 4.取出数据
            int len = packet.getLength();
//            String rs = new String(buffer,0,len);
            String rs = new String(buffer,0,len);
//            float x = bytes2Float(buffer);
            System.out.println("收到的数据:" + rs);
//            System.out.println("收到的数据:" + x);
            // 获取发送端的ip和端口
            String ip = packet.getSocketAddress().toString();
            System.out.println("发送端的IP地址: " + ip);

            int port = packet.getPort();
            System.out.println("发送端端口为: "+port);
        }


        // 关闭管道
//        socket.close();
    }
}
