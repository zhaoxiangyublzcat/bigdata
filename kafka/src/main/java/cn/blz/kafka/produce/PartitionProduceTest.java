package cn.blz.produce;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @author Blzcat
 * @description 分区kafka生产者测色
 * @className PartitionProduceTest
 * @date 2020.01.09 09:41
 */
public class PartitionProduceTest {
    public static void main(String[] args) {
        // 1. 创建kafka配置信息
        Properties properties = new Properties();
        // 1.1 key，value序列化类
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        // 1.2 自定义分区
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "cn.blz.partition.PartitionTest");

        // 2. 创建kafkaProducer生产者对象
        KafkaProducer<String, String> stringStringKafkaProducer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 10; i++) {
            stringStringKafkaProducer.send(new ProducerRecord<String, String>("test1", "tV--" + i), new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        System.out.println(recordMetadata.partition() + "---" + recordMetadata.offset());
                    } else {
                        e.printStackTrace();
                    }
                }
            });
        }

        // 3. 关闭资源
        stringStringKafkaProducer.close();
    }
}
