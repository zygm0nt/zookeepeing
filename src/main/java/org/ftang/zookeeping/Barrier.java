package org.ftang.zookeeping;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

/**
 * Barrier
 */
public class Barrier extends SyncPrimitive {
    
    private static Logger log = Logger.getLogger(Barrier.class);
    int size;
    String name;

    /**
     * Barrier constructor
     *
     * @param address
     * @param root
     * @param size
     */
    Barrier(String address, int port, String root, int size) {
        super(address, port);
        this.root = root;
        this.size = size;

        // Create barrier node
        if (zk != null) {
            try {
                Stat s = zk.exists(root, false);
                if (s == null) {
                    zk.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
            } catch (KeeperException e) {
                log.debug("Keeper exception when instantiating queue: " + e.toString());
            } catch (InterruptedException e) {
                log.debug("Interrupted exception");
            }
        }

        // My node name
        try {
            name = new String(InetAddress.getLocalHost().getCanonicalHostName().toString());
        } catch (UnknownHostException e) {
            log.debug(e.toString());
        }
    }

    /**
     * Join barrier
     *
     * @return
     * @throws org.apache.zookeeper.KeeperException
     * @throws InterruptedException
     */

    boolean enter() throws KeeperException, InterruptedException{
        zk.create(root + "/" + name, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        while (true) {
            synchronized (mutex) {
                List<String> list = zk.getChildren(root, true);

                if (list.size() < size) {
                    mutex.wait();
                } else {
                    return true;
                }
            }
        }
    }

    /**
     * Wait until all reach barrier
     *
     * @return
     * @throws org.apache.zookeeper.KeeperException
     * @throws InterruptedException
     */

    boolean leave() throws KeeperException, InterruptedException{
        zk.delete(root + "/" + name, 0);
        while (true) {
            synchronized (mutex) {
                List<String> list = zk.getChildren(root, true);
                if (list.size() > 0) {
                    mutex.wait();
                } else {
                    return true;
                }
            }
        }
    }
}