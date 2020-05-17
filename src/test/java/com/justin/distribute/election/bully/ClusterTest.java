package com.justin.distribute.election.bully;

import com.justin.distribute.election.bully.common.PropertiesUtil;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public class ClusterTest {
    @Test
    public void testClusterSync() throws InterruptedException {
        CountDownLatch cdl = new CountDownLatch(1);

        for (String nodeAddr : PropertiesUtil.getNodesAddress()) {
            String[] addr = nodeAddr.split(":");
            final String host = addr[0];
            final int port = Integer.parseInt(addr[1]);

            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    Node node = new Node(host, port);
                    node.start();
                }
            });
            thread.start();
        }

        cdl.await();
    }
}
