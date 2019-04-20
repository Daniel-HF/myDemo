package com.daniel.zookeeper;

import com.alibaba.fastjson.JSON;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class ZookeeperTest {

    private static final String zkserverPath = "centos02:2181,centos03:2181,centos03:2181";
    private static final int timeOut = 2000;
    private static final String nodePath = "/zkCachData";
    private ZooKeeper zkCli = null;


    /**
     * 初始化zk 客户端
     *
     * @throws IOException
     */
    @Before
    public void init () throws IOException {
        zkCli = new ZooKeeper (zkserverPath, timeOut, new Watcher () {
            public void process (WatchedEvent watchedEvent) {
                System.out.println ("监听到了事件........." + JSON.toJSONString (watchedEvent));



            }
        });
    }

    public static void main (String[] args) throws InterruptedException {


        Thread.sleep (Long.MAX_VALUE);
    }


    /**
     * 增加zk节点
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void createZkNode () throws KeeperException, InterruptedException {
        String resutlNodePath = zkCli.create (nodePath, "1111111111".getBytes (), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println ("zk节点添加成功：" + resutlNodePath);
        Thread.sleep (Integer.MAX_VALUE);
    }

    /**
     * 校验节点是否存在
     *
     * @throws Exception
     */
    @Test
    public void nodeExits () throws Exception {
        Stat exists = zkCli.exists (nodePath, false);
        System.out.println ("Exists:" + exists);

    }

    /**
     * 修改zk节点数据
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void setZKNodeData () throws Exception {
        zkCli.setData (nodePath, "1111111111".getBytes (), -1);
        byte[] data = zkCli.getData (nodePath, false, null);
        System.out.println ("data:" + new String (data));
    }


    /**
     * 获取zk节点数据
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void getZKNodeData () throws Exception {
        byte[] data = zkCli.getData (nodePath, false, new Stat ());
        System.out.println ("data：" + new String (data, "utf-8"));

    }


    @Test
    public void getAcl () throws KeeperException, InterruptedException {
        List<ACL> acl = zkCli.getACL (nodePath, new Stat ());
        for (ACL acl1 : acl) {
            System.out.println ("acl1:" + JSON.toJSONString (acl1));
        }
    }

    /**
     * 删除zk节点
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void deleteZkNode () throws KeeperException, InterruptedException {
        //参数2 表示要删除的版本，-1 是删除所有版本（但通常我们都不知道自己有多少个版本）
        zkCli.delete (nodePath, -1);
        System.out.println ("删除zk节点成功。。");
    }

}
