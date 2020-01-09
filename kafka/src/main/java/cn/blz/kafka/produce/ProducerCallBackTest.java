package cn.blz.produce;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @author Blzcat
 * @description kafka带回调函数的测试类
 * @className ProducerCallBackTest
 * @date 2020.01.08 21:22
 */
public class ProducerCallBackTest {
    public static void main(String[] args) {
        // 1. 创建kafka配置信息
        Properties properties = new Properties();
        // 1.1 key，value序列化类
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

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
