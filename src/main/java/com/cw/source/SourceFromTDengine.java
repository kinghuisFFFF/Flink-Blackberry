package com.cw.source;

import com.cw.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;



public class SourceFromTDengine extends RichSourceFunction<WaterSensor> {


    Statement statement;
    private Connection connection;
    private String property;

    public SourceFromTDengine(){
        super();
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        String driver = "com.taosdata.jdbc.rs.RestfulDriver";
        String host = "u05";
        String username = "root";
        String password = "taosdata";
        String prop = System.getProperty("java.library.path");
        Logger LOG = LoggerFactory.getLogger(SourceFromTDengine.class);
        LOG.info("java.library.path:{}", prop);
        System.out.println(prop);
        Class.forName( driver );
        Properties properties = new Properties();
        connection = DriverManager.getConnection("jdbc:TAOS-RS://" + host + ":6041/tt" + "?user=root&password=taosdata"
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
    public void run(SourceContext<WaterSensor> sourceContext) throws Exception {
        try {
            Logger LOG = LoggerFactory.getLogger(SourceFromTDengine.class);
            String sql = "select * from tt.meters";
            ResultSet resultSet = statement.executeQuery(sql);
            
            while (resultSet.next()) {
                WaterSensor WaterSensor = new WaterSensor(
                        resultSet.getString( "id" ).trim(),
                        resultSet.getLong( "ts" ),
                        resultSet.getInt( "vc" ));
                LOG.info("This message contains {} placeholders. {}", 2,WaterSensor);

                sourceContext.collect( WaterSensor );
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cancel() {

    }
}   