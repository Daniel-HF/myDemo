package com.daniel.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.ArrayList;
import java.util.List;

/**
 * zk 客户端
 *
 * @Author: Daniel.huang
 * @Date: 2019/4/18 11:34
 */
public class ZkClient {

//    private static final String zkUrl = "mini2:2181,mini3:2181,mini4:2181";
    private static final String zkUrl = "centos02:2181,centos03:2181,centos03:2181";
    private static final int timeOut = 2000;
    private static final String parentPath = "/zk";
    private ZooKeeper zkCli = null;
    /**
     * 加volatile 是为了这个变量的可见性，当有事件触发去改动这个变量的时候，别的地方也会相应的感知到，
     * 当有服务挂掉后，触发事件，然后会重新获取服务列表，然后重新赋值给当前变量，如果不加这个关键字，可能会
     * 导致业务系统调用到已经挂掉的服务。
     */
    private volatile List<String> zkServerList;

    public static void main(String[] args) throws Exception {
        /**
         * 客户端逻辑：
         *  1.连接zk
         *  2.获取服务列表
         *  3.调用服务
         */
        ZkClient zkClient = new ZkClient();
        zkClient.getConnect();
        zkClient.getServerList();
        zkClient.invokerServer();


    }

    /**
     * 获取zk connect
     */
    public void getConnect() throws Exception {
        zkCli = new ZooKeeper (zkUrl, timeOut, new Watcher () {
            /**
             * 监听器只有一次生命周期，如果想持续监听，需在获得监听后再次监听
             * @param watchedEvent
             */
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("zk node 监听事件。。。。");
                try {
                    //有事件触发，就获取服务
                    getServerList();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    /**
     * 获取服务列表
     */
    public void getServerList() throws Exception {

        zkCli.getChildren(parentPath, new Watcher () {
            @Override
            public void process(WatchedEvent watchedEvent) {
                /**
                 * 获取所有服务（获取子节点里的数据）
                 */
                try {
                    List<String> children = zkCli.getChildren(parentPath, true);
                    List<String> serverList = new ArrayList<>();
                    for (String child : children) {
                        //打印节点名
//                        System.out.println(child);
                        // 获取节点数据,无需监听
                        byte[] data = zkCli.getData(parentPath + "/" + child, false, null);
                        String serverName = data.toString();
                        serverList.add(serverName);
                    }
                    zkServerList = serverList;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

    }

    /**
     * 调用服务
     */
    public void invokerServer() throws InterruptedException {
        System.out.println("开始调用服务了。。。。");
        Thread.sleep(Long.MAX_VALUE);
    }


}
