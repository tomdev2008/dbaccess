package sirius.dbresource;

import java.sql.Connection;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.apache.log4j.BasicConfigurator;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import sirius.dbaccess.Order;
import sirius.dbaccess.OrderedRunner;

@RunWith(OrderedRunner.class)
public class TestConnection extends TestCase {

    static {
        BasicConfigurator.configure();
    }

    @Test
    @Order(order = 1)
    public void testGetReadConnection() {
        System.out.println("testGetReadConnectioin");
        String bizName = "buddy";
        try {
            Connection readConn = DbConfigManager.getInstance().getConfig(bizName)
                    .getReadConnection("buddy_18");
            System.out.println("readConn:" + readConn);
            Assert.assertNotNull(readConn);
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // @Test
    @Order(order = 2)
    public void testGetWriteConnection() {
        // System.out.println("testGetWriteConnection");
    }

    // @Test
    @Order(order = 3)
    public void sleepAndExit() {
        try {
            TimeUnit.SECONDS.sleep(8);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
