package com.cw.sink;

import com.cw.bean.TaosBean;
import com.cw.bean.TaosConfig;
import com.cw.bean.WaterSensor;
import com.cw.utils.YamlUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;


public class SinkToTDengine extends RichSinkFunction<WaterSensor> {
    Statement statement;
    private Connection connection;
    TaosConfig taosConfig = YamlUtils.yml2Bean("taos-conf.yml");

    public SinkToTDengine() {
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        TaosBean taos = taosConfig.getTaos();
        String driver = taos.getDriver();
        String host = taos.getHost();
        String username = taos.getUname();
        String password = taos.getPwd();
        String prop = System.getProperty("java.library.path");
        System.out.println(prop);
        Class.forName( driver );
        Properties properties = new Properties();
        connection = DriverManager.getConnection("jdbc:TAOS-RS://" + host + ":6041/test02" + "?timezone=UTC+8&user="+username+"&password="+password
                , properties);
        statement = connection.createStatement();
        
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
        if (statement != null) {
            statement.close();
        }
    }

    @Override
    public void invoke(WaterSensor WaterSensor, Context context) throws Exception {
        try {
            String sql = String.format("insert into test02.%s using test02.meters01 tags('%s') values(%d,%d)",
                                WaterSensor.getId(),
                                WaterSensor.getId(),
                                WaterSensor.getTs(),
                                WaterSensor.getVc()
                                );
            statement.executeUpdate(sql);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}