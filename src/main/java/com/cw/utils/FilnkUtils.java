package com.cw.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Title:
 * @BelongProjecet Flink-Blackberry
 * @BelongPackage com.cw.utils
 * @Description:
 * @Copyright time company - Powered By 研发一部
 * @Author: cw
 * @Date: 2023/6/10 11:38
 * @Version V1.0
 */
public class FilnkUtils {
    public static void main(String[] args) {
//        StreamExecutionEnvironment env = getStreamExecutionEnvironmentDev();
        StreamExecutionEnvironment env = getExecutionEnvironmentPro();
        env.setParallelism(1);

    }

    public static StreamExecutionEnvironment getExecutionEnvironmentPro() {
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }

    public static StreamExecutionEnvironment getStreamExecutionEnvironmentDev() {
        // TODO 1. 创建执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // IDEA运行时，也可以看到webui，一般用于本地测试
        // 需要引入一个依赖 flink-runtime-web
        Configuration configuration = new Configuration();
//        configuration.setString("rest.bind-port", "8081");
        //指定 Flink Web UI 端口为9091
//        configuration.setInteger("rest.port", 8082);
//        configuration.setInteger(RestOptions.PORT, 8081);
//        configuration.setString("rest.port","9091-9099"); //指定 Flink Web UI 端口为9091
//        configuration.setString("rest.port","9093"); //指定 Flink Web UI 端口为9091
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        return env;
    }
}
