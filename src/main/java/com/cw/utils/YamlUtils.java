package com.cw.utils;

import com.cw.bean.TaosConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.util.LinkedHashMap;
import java.util.Map;

public class YamlUtils {
    private static final Logger logger = LoggerFactory.getLogger(YamlUtils.class);

    // yml2Map: Yaml to Map, 将yaml文件读为map
    public static Map<String, Object> yml2Map(String path) throws FileNotFoundException {
        InputStream inputStream = YamlUtils.class.getClassLoader().getResourceAsStream(path);
//        FileInputStream fileInputStream = new FileInputStream(path);
        Yaml yaml = new Yaml();
        Map<String, Object> ret = (Map<String, Object>) yaml.load(inputStream);
        return ret;
    }

    public static TaosConfig yml2Bean(String path){
        TaosConfig ret = null;
        try {
            InputStream inputStream = YamlUtils.class.getClassLoader().getResourceAsStream(path);
            Yaml yaml = new Yaml();
            ret = yaml.loadAs(inputStream, TaosConfig.class);
        } catch (Exception e) {
            logger.error(path+"文件不存在,请仔细检查");
            e.printStackTrace();
        }
        return ret;
    }

    // map2Yml: Map to Yaml, 将map转换为yaml格式
    public static void map2Yml(Map<String, Object> map, String path) throws IOException {

        File file = new File(path);
        FileWriter fileWriter = new FileWriter(file);
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        Yaml yaml = new Yaml(options);
        yaml.dump(map, fileWriter);
    }



    public static void main(String[] args) throws IOException {
//        m1();
        m2();
//        m3();
    }



    private static void m3() {

    }

    private static void m2() throws FileNotFoundException {
//        Map conf = YamlUtils.yml2Map("kafka-conf.yml");
//        TaosConfig conf2 = YamlUtils.yml2Bean("kafka-conf.yml");
        TaosConfig conf2 = YamlUtils.yml2Bean("taos-conf.yml");
//        Map conf = (Map) new Yaml().load(new FileInputStream(new File("./kafka-conf.yml")));
        System.out.println(conf2);  // test_gp2
//        System.out.println(conf.get("kafka"));  // test_gp2
//        System.out.println(conf.get("taos.driver"));  // test_gp2
//        System.out.println(conf.get("taos.host"));  // test_gp2
//        System.out.println(conf.get("taos.uname"));  // test_gp2
//        System.out.println(conf.get("taos.pwd"));  // test_gp2
//        System.out.println(conf.get("test.param"));  // test_gp2
//        System.out.println(conf.get("kafka.bootstrap.servers"));  // test_gp2
//        System.out.println(conf.getOrDefault("test.param2", "unknown"));  // unknown
    }



    private static void m1() throws IOException {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("name", "张三");
        data.put("age", "25");
        data.put("mobile", new String[] { "15012345678", "18745612378" });
        Yaml yaml = new Yaml();
        FileWriter writer = new FileWriter("map2.yaml");
        yaml.dump(data, writer);
        writer.close();
        System.out.println(writer.toString());
    }
}
