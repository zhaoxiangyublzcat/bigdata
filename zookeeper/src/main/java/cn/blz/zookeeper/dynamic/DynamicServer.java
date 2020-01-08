package cn.blz.zookeeper.dynamic;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * @Description zookeeper动态服务器
 * @ClassName DynamicServer
 * @Author Blzcat
 * @date 2020.01.01 21:30
 */
public class DynamicServer {
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        DynamicServer dynamicServer = new DynamicServer();
        // 1. 链接服务器（zookeeper集群）
        dynamicServer.getConnect();
        // 2. 注册节点
        dynamicServer.regist(args[0], args[1]);
        // 3. 业务逻辑处理
        dynamicServer.doWhile();
    }


    private static String connectString = "bio.cloudera.com:2181,bio.cloudera2.com:2181,bio.cloudera3.com:2181";
    private ZooKeeper zooKeeper = null;

    /*
     * @param
     * @return  void
     * @Description 具体业务逻辑
     * @Author  Blzcat
     * @Date    2020.01.01 22:12
     **/
    private void doWhile() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    /*
     * @param	path
     * @param	data
     * @return  void
     * @Description 注册服务
     * @Author  Blzcat
     * @Date    2020.01.01 22:12
     **/
    private void regist(String path, String data) throws KeeperException, InterruptedException {
        zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    /*
     * @param
     * @return  void
     * @Description 获取zookeeper集群链接
     * @Author  Blzcat
     * @Date    2020.01.01 22:11
     **/
    private void getConnect() throws IOException {
        zooKeeper = new ZooKeeper(connectString, 2000, new Watcher() {
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }
}
