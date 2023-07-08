package com.cw.test;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @Title:
 * @BelongProjecet Flink-Blackberry
 * @BelongPackage com.cw.test
 * @Description:
 * @Copyright time company - Powered By 研发一部
 * @Author: cw
 * @Date: 2023/7/5 17:35
 * @Version V1.0
 */
@Slf4j
public class A {
    public static void main(String[] args) {
        List<String> list = new ArrayList<>();
        list.add("1");
        list.add("2");
        list.add("index");
        list.add("3");
        log.error("-------原来的list:{}", list.stream().collect(Collectors.joining(",")));
        list.set(1, "5");
        log.error("-------把下标1更为5后的list:{}", list.stream().collect(Collectors.joining(",")));
    }

    /**
     *
     *
     */

}
