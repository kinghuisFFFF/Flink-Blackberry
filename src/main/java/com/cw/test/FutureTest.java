package com.cw.test;

import java.util.concurrent.*;

public class FutureTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
//        m1();
//        m2();
//        m3();
        String s1="CREATE TABLE hbase_order_labels (\n" +
                " rowkey string,\n" +
                " c ROW<match_num_h string>,\n" +
                " PRIMARY KEY (rowkey) NOT ENFORCED" +
                ") WITH (" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'rtc_dws:rtc_dws_order_labels',\n" +
                " 'zookeeper.quorum' = 'server:2181',\n" +
                " 'zookeeper.znode.parent' = '/hbase-unsecure'" +
                ")";

        System.out.println(s1);
        System.out.println(s1);

    }

    private static void m3() throws InterruptedException, ExecutionException {
        // 核心线程池大小5 最大线程池大小10 线程最大空闲时间60 时间单位s 线程等待队列
        ThreadPoolExecutor executor = new ThreadPoolExecutor(5, 10, 60L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(10));
        CompletableFuture<Long> future = CompletableFuture
                // 执行异步任务
                .supplyAsync(() -> {
                    return System.currentTimeMillis();
                }, executor)
                // 对前面的结果进行处理
//                .thenApply(n -> {
                .thenApplyAsync(n -> {
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    Long time = System.currentTimeMillis();
                    System.out.println("如果是同步的，这条消息应该先输出");
                    return time-n;
                });
        System.out.println("等待2秒");
        System.out.println(future.get());
        executor.shutdown();
    }

    private static void m2() throws InterruptedException, ExecutionException {
        // 核心线程池大小5 最大线程池大小10 线程最大空闲时间60 时间单位s 线程等待队列
        ThreadPoolExecutor executor = new ThreadPoolExecutor(5, 10, 60L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(10));
        CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {
            return "hello";
        }, executor);
        System.out.println(future.get());
        executor.shutdown();
    }

    private static void m1() throws InterruptedException, ExecutionException {
        // 核心线程池大小5 最大线程池大小10 线程最大空闲时间60 时间单位s 线程等待队列
        ThreadPoolExecutor executor = new ThreadPoolExecutor(5, 10, 60L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(10));
        Future<Long> future = executor.submit(() -> {
            // 故意耗时
            Thread.sleep(3000);
            return System.currentTimeMillis();
        });
        System.out.println(future.get());
        System.out.println("因为get是阻塞的，所以这个消息在数据之后输出");
        executor.shutdown();
    }
}

