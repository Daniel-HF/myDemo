package com.daniel.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * zk 服务注册
 *
 * @Author: Daniel.huang
 * @Date: 2019/4/18 10:25
 */
public class ZkServerRegister {

    //    private static final String zkUrl = "mini2:2181,mini3:2181,mini4:2181";
    private static final String zkUrl = "centos02:2181,centos03:2181,centos03:2181";
    private static final int timeOut = 1000;
    private static final String parentPath = "/zk";
    private static final String thisNode = "/sub";
    private ZooKeeper zkCli = null;

    /**
     * 业务系统注册服务到zk
     *
     * @param args
     */
    public static void main (String[] args) throws Exception {
        //获取zk 连接
        ZkServerRegister register = new ZkServerRegister ();
        register.getConnect ();
        //注册zk服务
        register.serverRegister (args[0]);
        //业务系统执行操作
        register.businessLogic (args[0]);
    }

    /**
     * 获取zk connect
     */
    public void getConnect () throws Exception {
        zkCli = new ZooKeeper (zkUrl, timeOut, new Watcher () {
            /**
             * 监听器只有一次生命周期，如果想持续监听，需在获得监听后再次监听
             * @param watchedEvent
             */
            @Override
            public void process (WatchedEvent watchedEvent) {
                System.out.println ("zk node 监听事件。。。。");
                //再次监听
                try {
                    zkCli.getChildren ("/", true);
                } catch (Exception e) {
                    e.printStackTrace ();
                }
            }
        });
    }

    /**
     * 创建zk 节点
     *
     * 参数1：
     * 参数2：
     * 参数3：
     * 参数4：
     */

    /**
     *
     * @param nodePath 节点路径
     * @param data 节点数据
     * @param acl  节点权限，有四钟，通常使用当前这种，表示开发所有权限
     * @param createMode 创建节点类型，有四种：持久的；持久带版本号的；临时的；临时带版本号的，通常为了节点的动态性，
     *                   会设置为临时带版本号的这样当一个节点挂掉后就可以剔除对应的服务
     * @return
     * @throws Exception
     */
    public String createNode (String nodePath, String data, List<ACL> acl, CreateMode createMode) throws Exception {
        return zkCli.create (nodePath, data.getBytes (), acl, createMode);
    }

    /**
     * 节点存在检查，因为zookeeper的节点只能一层一层创建
     *
     * @throws Exception
     */
    public boolean exists (String parentPath) throws Exception {
        boolean result = false;
        Stat exists = zkCli.exists (parentPath, false);
        if (exists != null) {
            result = true;
        }
        return result;
    }


    /**
     * 注册服务
     *
     * @param hostName
     *
     * @throws Exception
     */
    public void serverRegister (String hostName) throws Exception {
        //判断父节点存不存在,不存在先创建父节点
        if (!exists (parentPath)) {
            String parentNode = createNode (parentPath, hostName,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
            System.out.println ("父节点创建成功...." + parentNode);
        }
        String childNode = createNode (parentPath + thisNode, hostName,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println ("子节点创建成功...."+childNode);

    }

    /**
     * 业务逻辑
     */
    public void businessLogic (String hostName) throws InterruptedException {
        //执行业务逻辑
        System.out.println (hostName + " 开始执行业务逻辑了。。。。");
        Thread.sleep (Long.MAX_VALUE);
    }

}
