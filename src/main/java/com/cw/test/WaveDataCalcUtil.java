package com.cw.test;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: 
 * @Date: 
 * @Description: 进行数据转换的工具类
 */
public class WaveDataCalcUtil {

    /**
     * 将byte类型的arr转换成float
     * @return
     */
    public static List<Float> byteArrayToFloatList(byte[] bytes){
        List<Float> d = new ArrayList<>(bytes.length/8);
        byte[] doubleBuffer = new byte[4];
        for(int j = 0; j < bytes.length; j += 4) {
            System.arraycopy(bytes, j, doubleBuffer, 0, doubleBuffer.length);
            d.add(bytes2Float(doubleBuffer));
        }
        return d;
    }

    /**
     * 将byte类型的arr转换成double
     * @param arr
     * @return
     */
    public static List<Double> byteArrayToDoubleList(byte[] arr){
        List<Double> d = new ArrayList<>(arr.length/8);
        byte[] doubleBuffer = new byte[8];
        for(int j = 0; j < arr.length; j += 8) {
            System.arraycopy(arr, j, doubleBuffer, 0, doubleBuffer.length);
            d.add(bytes2Double(doubleBuffer));
        }
        return d;
    }

    /**
     * 将byte数组数据转换成float
     * @param arr
     * @return
     */
    public static float bytes2Float(byte[] arr) {
        int accum = 0;
        accum = accum|(arr[0] & 0xff) << 0;
        accum = accum|(arr[1] & 0xff) << 8;
        accum = accum|(arr[2] & 0xff) << 16;
        accum = accum|(arr[3] & 0xff) << 24;
        return Float.intBitsToFloat(accum);
    }

    /**
     * 将byte转换成double
     * @param arr
     * @return
     */
    public static double bytes2Double(byte[] arr) {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value |= ((long) (arr[i] & 0xff)) << (8 * i);
        }
        return Double.longBitsToDouble(value);
    }
}

