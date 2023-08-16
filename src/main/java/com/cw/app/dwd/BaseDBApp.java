package com.cw.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cw.app.func.DimSink;
import com.cw.app.func.MyDeserializationSchemaFunction;
import com.cw.app.func.TableProcessFunction;
import com.cw.bean.TableProcess;
import com.cw.utils.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

//数据流：Web/App -> Nginx -> SpringBoot ->  Mysql -> FlinkAPP ->  Kafka(ods) -> FlinkAPP -> Kafka(dwd)/Phoenix(dim)
//数据流：Web/App -> Nginx -> SpringBoot ->  Mysql -> FlinkAPP ->  Kafka(ods_base_db) -> FlinkAPP -> Kafka(dwd)/Phoenix(dim)
//进程:           MockDB                 -> Mysql ->  FlinkCDCApp -> Kafka(ZK) -> BaseDBApp3 -> Kafka/Phoenix(hbase,zk,hdfs)
public class BaseDBApp {
    static String simpleClassName = BaseDBApp.class.getSimpleName();
    public  static void main(String[] args) throws Exception {

        //TODO 1 获取执行环境
//        StreamExecutionEnvironment env = FlinkUtils.getStreamExecutionEnvironmentByDev();
//        StreamExecutionEnvironment env = FlinkUtils.getStreamExecutionEnvironmentByPro();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //并行度设置应该与Kafka主题的分区数一致

        //1.2 设置CK&状态后端 生产环境必须有这些代码
//        env.enableCheckpointing(5000L); // 5s
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000L); // 1min,时间太短容易报错。之前设置1s经常报错 10000L
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));
//        env.setStateBackend(new FsStateBackend("hdfs://edh/flink/job3/ck/"));
//        //设置可容忍的检查点失败数，默认值为0表示不允许容忍任何检查点失败
//        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
//        System.setProperty("HADOOP_USER_NAME", "hdfs");

        //TODO 2 读取Kafka ods_base_db 主题数据创建流
        String groupId = "base_db_app_group";
        String topic = "ods_base_db";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(groupId, topic);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);
        kafkaDS.print("kafka数据");
        //TODO 3 过滤空值+delete 数据(主流)
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject)
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        String type = jsonObject.getString("type");
//                        if("delete".equals(type)){
//                            return true;
//                        }
//                        return false;
                        return !"delete".equals(type);
                    }
                });

//        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
//            @Override
//            public void processElement(String value, Context context, Collector<JSONObject> collector) throws Exception {
//                try {
//                    JSONObject jsonObject = JSON.parseObject(value);
//                    collector.collect(jsonObject);
//                } catch (Exception e) {
//                    System.out.println("发现脏数据。。。。" + value);
//                }
//            }
//        });

        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(jsonObj -> {
            String data = jsonObj.getString("data");
            return data != null && data.length() > 0;
        });

        //TODO 4 使用FlinkCDC读取配置表形成广播流
        MySqlSource<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("mydb85") //指定监控的哪台服务器（MySQL安装的位置）
                .port(21020) //MySQL连接的端口号
                .username("r_tom") //用户
                .password("clOloXJEtg3WlydR")//密码
                .databaseList("db_kafka2") //list：可以监控多个库
                .tableList("db_kafka2.table_process") // set captured table
                .deserializer(new MyDeserializationSchemaFunction())
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> mysqlDS = env.fromSource(sourceFunction, WatermarkStrategy.noWatermarks(), "MysqlSource");

        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("table-process", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = mysqlDS.broadcast(mapStateDescriptor);
        //TODO 5 连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = filterDS.connect(broadcastStream);
        //TODO 6 分流
        OutputTag<JSONObject> hbaseOutPutTag = new OutputTag<JSONObject>("hbase") {
        };
        SingleOutputStreamOperator<JSONObject> resultDS = connectedStream.process(new TableProcessFunction(hbaseOutPutTag, mapStateDescriptor));
        //TODO 7 将分好的流写入Phoenix表(维度数据)或者Kafka主题(事实数据)
        filterDS.print("主流原始数据>>>>>>>>");
        resultDS.print("Kafka>>>>>>>>");
        resultDS.getSideOutput(hbaseOutPutTag).print("HBase>>>>>>>>>>>");

        //将数据写入Phoenix
        resultDS.getSideOutput(hbaseOutPutTag).addSink(new DimSink());

        //将数据写入Kafka
//        resultDS.map(jsonObject -> jsonObject.toJSONString()).addSink(MyKafkaUtil.getKafkaSink("xx"));

        FlinkKafkaProducer<JSONObject> kafkaSinkBySchema = MyKafkaUtil.getKafkaSinkBySchema2(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public void open(SerializationSchema.InitializationContext context) throws Exception {
                System.out.println("开始序列化Kafka数据");
            }

            //element:{"database":"","table":"","type":"","data":{"id":"11"...},"sinkTable":"dwd_xxx_xxx"}
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                return new ProducerRecord<>(element.getString("sinkTable"),
                        element.getString("data").getBytes());
            }
        });
        resultDS.addSink(kafkaSinkBySchema);

        //TODO 8 启动任务
        env.execute(simpleClassName);
    }

}
