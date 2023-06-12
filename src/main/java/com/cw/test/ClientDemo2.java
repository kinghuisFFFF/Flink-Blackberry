package com.cw.test;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Scanner;

public class ClientDemo2 {
    /**
     * 发送端
     */
    public static void main(String[] args) throws Exception {
        System.out.println("=============发送端启动===========");
        // 1.创建发送端对象
        DatagramSocket socket = new DatagramSocket(6666);
        Scanner sc = new Scanner(System.in);

        while (true) {
            System.out.println("请输入您要发送的消息: ");
            String msg = sc.nextLine();

            if("exit".equals(msg)){
                System.out.println("退出成功!");
                socket.close();
                break;
            }

            // 2.创建一个数据包对象封装数据
            byte[] buffer = msg.getBytes();
            DatagramPacket packet = new DatagramPacket(buffer,buffer.length, InetAddress.getByName("255.255.255.255"),9999);

            // 3.发送数据
            socket.send(packet);
        }
    }
}
