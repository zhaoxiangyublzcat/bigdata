package cn.blz.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author Blzcat
 * @description 自定义分区
 * @className PartitionTest
 * @date 2020.01.09 08:48
 */
public class PartitionTest implements Partitioner {
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        Integer integer = cluster.partitionCountForTopic(s);
        return o.toString().hashCode() % integer;
    }

    public void close() {

    }

    public void configure(Map<String, ?> map) {

    }
}
