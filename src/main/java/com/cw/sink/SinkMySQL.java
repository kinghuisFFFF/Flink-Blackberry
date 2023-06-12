package com.cw.sink;

import com.cw.bean.MyGeneratorFunction;
import com.cw.bean.MyGeneratorFunction2;
import com.cw.bean.TrafficData2;
import com.cw.bean.WaterSensor;
import com.cw.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 */
public class SinkMySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


//        SingleOutputStreamOperator<WaterSensor> sensorDS = env
//                .socketTextStream("tencentcloud.yawujia.cn", 8082)
//                .map(new WaterSensorMapFunction());

        DataGeneratorSource dataGeneratorSource = new DataGeneratorSource(new MyGeneratorFunction2(), Long.MAX_VALUE, Types.POJO(WaterSensor.class));
        SingleOutputStreamOperator sensorDS = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator")
                // 指定返回类型
                .returns(new TypeHint<WaterSensor>() {
                });

        /**
         * TODO 写入mysql
         * 1、只能用老的sink写法： addsink
         * 2、JDBCSink的4个参数:
         *    第一个参数： 执行的sql，一般就是 insert into
         *    第二个参数： 预编译sql， 对占位符填充值
         *    第三个参数： 执行选项 ---》 攒批、重试
         *    第四个参数： 连接选项 ---》 url、用户名、密码
         */
        SinkFunction<WaterSensor> jdbcSink = JdbcSink.sink(
                "insert into ws(`id`, `ts`, `vc`) values(?,?,?)",
                new JdbcStatementBuilder<WaterSensor>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, WaterSensor waterSensor) throws SQLException {
                        //每收到一条WaterSensor，如何去填充占位符
                        preparedStatement.setString(1, waterSensor.getId());
                        preparedStatement.setLong(2, waterSensor.getTs());
                        preparedStatement.setInt(3, waterSensor.getVc());
                    }
                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(3) // 重试次数
                        .withBatchSize(100) // 批次的大小：条数
                        .withBatchIntervalMs(3000) // 批次的时间
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://127.0.0.1:3306/test?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8")
                        .withUsername("root")
                        .withPassword("root")
                        .withConnectionCheckTimeoutSeconds(60) // 重试的超时时间
                        .build()
        );


        sensorDS.addSink(jdbcSink);


        env.execute();
    }
}
