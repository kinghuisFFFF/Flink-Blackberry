package com.cw.test;
 
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.Scanner;
 
/*使用UDP协议接收数据的步骤
 * 	1、创建数据接收端Socket对象 DtagramSocket（端口号）
 * 	2、创建一个数据包DatagramPacket对象用于接收数据 DataGramPacket(byte[]存储接收对象，长度)调用DatagramSocket中的方法receive（）接收数据
 * 	3、解析数据 DatagramPacket中方法getData（）放回缓冲区byte【】
 * 	4、关闭接收端*/
public class T {
	public static void main(String agrs[]) throws IOException {
//		m1();
		System.out.println("=============客户端启动===========");

		// 1.创建接受对象
		DatagramSocket socket = new DatagramSocket(6667);

		// 2.创建一个数据包接收数据
		byte [] buffer = new byte[1024];
		DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

		while (true) {

			// 3.等待接受数据
			socket.receive(packet);

			// 4.取出数据
			int len = packet.getLength();
			String rs = new String(buffer,0,len);
			System.out.println("收到来自: "+ packet.getAddress()+ ",对方端口号为: "+ packet.getPort()+"的消息: " + rs);
		}

	}

	private static void m1() throws IOException {
		Scanner sc=new Scanner(System.in);
		String m=sc.next();

		//创建接收端Socket对象 DatagramSocket对象，并绑定端口号用于监听
		DatagramSocket datagramSocket=new DatagramSocket(10086);
		//接收数据
		byte buf[]=new byte[1024];
		DatagramPacket datagramPacket=new DatagramPacket(buf,buf.length);
		datagramSocket.receive(datagramPacket);
		//解析数据
		byte buu[]=datagramPacket.getData();
		String buuString=new String(buu);
		System.out.println("收到："+buuString);
	}
}