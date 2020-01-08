package cn.blz.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @Description zookeeper客户端
 * @ClassName ZookeeperTest
 * @Author Blzcat
 * @date 2020.01.01 21:05
 */
public class ZookeeperTest {
    static String connectString = "bio.cloudera.com:2181,bio.cloudera2.com:2181,bio.cloudera3.com:2181";
    ZooKeeper zooKeeper = null;

    /**
     * @Description 初始化链接
     * @Author Blzcat
     * @date 2020.01.01 21:11
     */
    @Before
    public void init() throws IOException {
        zooKeeper = new ZooKeeper(connectString, 2000, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                // 动态监听所添加部分
            }
        });
    }

    /**
     * @description 创建节点
     * @author Blzcat
     * @date 2020.01.08
     */
    @Test
    public void create() throws KeeperException, InterruptedException {
        zooKeeper.create("/a/b.txt", "测试数据".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    /**
     * @description 获取子节点并监听数据
     * @author Blzcat
     * @date 2020.01.08
     */
    @Test
    public void getChildrensAndWatching() throws KeeperException, InterruptedException {
        for (String child : zooKeeper.getChildren("/a/b.txt", true)) {
            System.out.println(child);
        }
        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * @description 判断节点是否存在
     * @author Blzcat
     * @date 2020.01.08
     */
    @Test
    public void isExist() throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists("/a/b.txt", true);
        System.out.println(stat == null ? "1" : "0");
    }


}
