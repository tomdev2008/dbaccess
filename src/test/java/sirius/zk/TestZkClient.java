package sirius.zk;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.BasicConfigurator;
import org.junit.Assert;
import org.junit.Test;

/**
 * 
 * @author liyong19861014@gmail.com
 * 
 */
public class TestZkClient {

    private static ZkClient zkClient;

    private static final String TEST_ZNODE = "/test";

    static {
        // config log4j
        BasicConfigurator.configure();

        // init zkClient
        try {
            zkClient = ZkClient.getInstance("localhost:2181");
            zkClient.addZnodeListener(new Listener(TEST_ZNODE));
            zkClient.addZnodeListener(new Listener(TEST_ZNODE));
            zkClient.addZnodeListener(new Listener(TEST_ZNODE));
        } catch (ZookeeperException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetZNode() throws Exception {
        Assert.assertNotNull(zkClient);
        List<String> children = zkClient.getNodes(TEST_ZNODE);
        System.out.println(children);
        TimeUnit.SECONDS.sleep(5);
    }

    static class Listener extends ZNodeListener {

        public Listener(String znode) {
            super(znode);
        }

        @Override
        public boolean update(List<String> childrenNameList) {
            System.out.println("znode:" + getZNode() + "\tchildren:" + childrenNameList);
            return true;
        }

    }

}
