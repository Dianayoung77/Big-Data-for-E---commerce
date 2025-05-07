package utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtil {
    public static Properties properties = null;

    static {
        ClassLoader classLoader = PropertiesUtil.class.getClassLoader();
        InputStream is = classLoader.getResourceAsStream("hbase_consumer.properties");
        if (is == null) {
            System.err.println("Properties file not found: hbase_consumer.properties");
            properties = new Properties();
        } else {
            properties = new Properties();
            try {
                properties.load(is);
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("Failed to load properties");
            }
        }
    }


    public static String getProperty(String key){
        return properties.getProperty(key);
    }
}