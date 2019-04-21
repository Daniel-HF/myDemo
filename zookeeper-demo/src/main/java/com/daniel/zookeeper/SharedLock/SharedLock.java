package com.daniel.zookeeper.SharedLock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * 用zk 实现分布式共享锁
 *
 * 1.链接zk
 * 2.注册当前机器
 * 3.获取锁：每个都注册为带编号的的临时的锁
 * 4.执行业务逻辑
 * 5.释放锁
 * 6.重新注册当前机器
 *
 */

public class SharedLock {

    private static final String zkUrl = "centos02:2181,centos03:2181,centos03:2181";
    private static final int timeOut = 1000;
    private static final String parentPath = "/lock";
    private static final String sub="/sub";
    private static String lockNumber=""; // 当前锁编号
    private ZooKeeper zkCli = null;


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
                // 需要判断下事件类型
                System.out.println("zk node 监听事件。。。。");
                try {
                    getLock ();
                } catch (Exception e) {
                    e.printStackTrace ();
                }
            }
        });
    }

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
        String childNode = createNode (parentPath + sub, hostName,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println ("子节点创建成功...."+childNode);

    }


    public void getLock() throws Exception {

        List<String> children = zkCli.getChildren (parentPath, true);
        if(children.size () ==1 && lockNumber.equals (children.get (0))) {
            //只有我一个人，我直接去执行业务操作了
            execcutBusiness ();
        } else if(lockNumber.equals (children.get (0))) {
            //如果有多个，但第一个还是我，我也去执行业务操作了
            execcutBusiness ();
        }
        //再次监听
        try {
            zkCli.getChildren(parentPath, true);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * 执行业务操作
     *
     * @throws InterruptedException
     */
    public void execcutBusiness() throws Exception {
        System.out.println ("这事只能我一个人干。。");
        Thread.sleep (3000);
        deleteNode ();
    }

    /**
     * 删除当前节点
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void deleteNode() throws KeeperException, InterruptedException {
        this.zkCli.delete (parentPath+ lockNumber,-1);
    }


    public static void main (String[] args) throws Exception {
        SharedLock lock = new SharedLock ();
        lock.getConnect ();
        lock.serverRegister (args[0]);
        Thread.sleep (Long.MAX_VALUE);
    }


}
