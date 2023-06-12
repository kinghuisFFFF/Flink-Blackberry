package com.cw.test;
import sun.misc.Unsafe;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public class BigSmallEndTest {
 
    public static void main(String[] args) {
        ByteBuffer bb = ByteBuffer.wrap(new byte[12]);
        // 存入字符串
        bb.asCharBuffer().put("abdcef");
        System.out.println(Arrays.toString(bb.array()));

        // 反转缓冲区
        bb.rewind();
        // 设置字节存储次序
        bb.order(ByteOrder.BIG_ENDIAN);
        bb.asCharBuffer().put("abcdef");
        System.out.println(Arrays.toString(bb.array()));

        // 反转缓冲区
        bb.rewind();
        // 设置字节存储次序
        bb.order(ByteOrder.LITTLE_ENDIAN);
        bb.asCharBuffer().put("abcdef");
        System.out.println(Arrays.toString(bb.array()));


//        m1();
    }

    private static void m1() {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            Unsafe UNSAFE = (Unsafe) theUnsafe.get(null);
            long a = UNSAFE.allocateMemory(4);
            UNSAFE.putInt(a, 0x01020304);
            //存放此int类型数据,实际存放占4个字节,01,02,03,04
            byte b = UNSAFE.getByte(a);
            //通过getByte方法获取刚才存放的int，取第一个字节
            //如果是大端，int类型顺序存放—》01,02,03,04,取第一位便是0x01
            //如果是小端，int类型顺序存放—》04,03,02,01,取第一位便是0x04
            ByteOrder byteOrder = null;
            switch (b) {
                case 0x01:
                    byteOrder = ByteOrder.BIG_ENDIAN;
                    break;
                case 0x04:
                    byteOrder = ByteOrder.LITTLE_ENDIAN;
                    break;
                default:
                    assert false;
                    byteOrder = null;
            }
            System.out.println(byteOrder);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    //将整数按照小端存放，低字节出访低位
    public static byte[] toLH(int n) {
        byte[] b = new byte[4];
        b[0] = (byte) (n & 0xff);
        b[1] = (byte) (n >> 8 & 0xff);
        b[2] = (byte) (n >> 16 & 0xff);
        b[3] = (byte) (n >> 24 & 0xff);
        return b;
    }

    /**
     * 将int转为大端，低字节存储高位
     *
     * @param n
     * int
     * @return byte[]
     */
    public static byte[] toHH(int n) {
        byte[] b = new byte[4];
        b[3] = (byte) (n & 0xff);
        b[2] = (byte) (n >> 8 & 0xff);
        b[1] = (byte) (n >> 16 & 0xff);
        b[0] = (byte) (n >> 24 & 0xff);
        return b;
    }

    /**
     * 将小端bytes数据转化为大端数据
     * <p>
     * 默认网络传输字节为大端，java 全部为大端（与平台无关）
     * 关于 “Little-Endian and Big-Endian”,详情请参考：
     *
     * @param bytes
     * @return 转化后得到的整数
     * @Link https://howtodoinjava.com/java/basics/little-endian-and-big-endian-in-java/
     * </p>
     */
    private int bytesToBigEndian(byte[] bytes) {
        int result = 0;
        if (bytes == null || bytes.length < 0)
            return -1;

        int RECORD_BYTES_SIZE = 1024;
        int PORT_BYTES_SIZE = 1024;
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.BIG_ENDIAN);
        if (bytes.length == RECORD_BYTES_SIZE) {
            result = buffer.getInt();
        } else if (bytes.length == PORT_BYTES_SIZE) {
            // 端口号：0 ~ 65535; Short: -32768 ~ 32767
            short tmp = buffer.getShort();
            result = tmp < 0 ? getUnsignedShort(tmp) : tmp;
        }
        if (result < 0) {
            System.out.println("Length = " + result + " ; original data:" + bytes);
        }
        return result;
    }

    public static int getUnsignedShort(short buf) {
        return buf & 0x0000FFFF;
    }

    public static int getUnsignedShort2(ByteBuffer buf) {
        return buf.getShort() & 0x0000FFFF;
    }

}