package cn.blz.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @description hdfs客户端
 * @className HdfsClient
 * @author Blzcat
 * @date 2019.12.27 18:23
 */
public class HdfsClient {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        Configuration configuration = new Configuration();
//        configuration.set("fs.defaultFS","hdfs://bio.cloudera.com:8020");
        // 获取hdfs客户端对象
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bio.cloudera.com:8020"), configuration, "hdfs");
        // 创建目录
        fileSystem.mkdirs(new Path("/tmp/hdfs-study2"));
        // 释放资源
        fileSystem.close();
    }
}
