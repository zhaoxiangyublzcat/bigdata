package cn.blz.zookeeper.dynamic;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * @Description zookeeper动态客户端
 * @ClassName DynamicClient
 * @Author Blzcat
 * @date 2020.01.01 22:10
 */
public class DynamicClient {
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        DynamicClient dynamicClient = new DynamicClient();
        // 1. 获取zookeeper集群
        dynamicClient.getConnect();
        // 2. 注册监听
        dynamicClient.getChildren("");
        // 3. 业务逻辑
        dynamicClient.doWhile();
    }

    private void doWhile() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    private void getChildren(String path) throws KeeperException, InterruptedException {
        ArrayList<String> hosts = new ArrayList<String>();
        for (String child : zooKeeper.getChildren(path, true)) {
            hosts.add(Arrays.toString(zooKeeper.getData(path + "/" + child, false, null)));
        }

        System.out.println(hosts);
    }

    private static String connectString = "bio.cloudera.com:2181,bio.cloudera2.com:2181,bio.cloudera3.com:2181";
    private ZooKeeper zooKeeper = null;

    private void getConnect() throws IOException {
        zooKeeper = new ZooKeeper(connectString, 2000, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                // 调用getChildren()方法
            }
        });
    }
}
