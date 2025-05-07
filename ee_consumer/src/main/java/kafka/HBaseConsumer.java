package kafka;

// API的方式进行消费
import hbase.HBaseDAO;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import utils.PropertiesUtil;

import java.util.Arrays;

public class HBaseConsumer {

    public static void main(String[] args) {
        //从PropertiesUtil加载所有Kafka配置（bootstrap.servers/group.id等）
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(PropertiesUtil.properties);
        //动态获取要消费的主题名称（示例值为callog-test）订阅主题
        kafkaConsumer.subscribe(Arrays.asList(PropertiesUtil.getProperty("kafka.topics")));
        // 创建HBaseDAO实例
        HBaseDAO hBaseDAO = new HBaseDAO();
        // 持续消费
        while (true) {
            // 每100ms拉取一次消息（超时机制防止阻塞）
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            // 遍历消息并打印
            for (ConsumerRecord<String, String> cr : records) {
                System.out.println(cr.value());

                // 将消息写入HBase
                hBaseDAO.put(cr.value());//这里需要将消息解析为HBase的rowkey和列族、列名、列值
            }
        }
    }
}
