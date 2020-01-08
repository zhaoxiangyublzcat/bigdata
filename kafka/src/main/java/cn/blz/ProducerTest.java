package cn.blz;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author Blzcat
 * @description kafka生产者测试类
 * @className ProducerTest
 * @date 2020.01.08 20:50
 */
public class ProducerTest {
    public static void main(String[] args) {
        // 1. 创建kafka配置信息
        Properties properties = new Properties();
        // 1.1 指定连接的kafka集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "bio.cloudera.com:9092");
        // 1.2 ack应答级别
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        // 1.3 重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        // 1.4 批次大小
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        // 1.5 等待时间，单位ms
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        // 1.6 RecordAccumulator缓冲区大小
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        // 1.7 key，value序列化类
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 2. 创建kafkaProducer生产者对象
        KafkaProducer<String, String> stringStringKafkaProducer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 10; i++) {
            stringStringKafkaProducer.send(new ProducerRecord<String, String>("test1", "tV--" + i));
        }

        // 3. 关闭资源
        stringStringKafkaProducer.close();
    }
}
