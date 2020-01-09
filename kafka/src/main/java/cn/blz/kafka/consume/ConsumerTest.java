package cn.blz.kafka.consume;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

/**
 * @author Blzcat
 * @description 消费者
 * @className CustomerTest
 * @date 2020.01.09 09:48
 */
public class ConsumerTest {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "bio.cloudera.com:9092");

        KafkaConsumer<String, String> stringStringKafkaConsumer = new KafkaConsumer<String, String>(properties);
    }
}
