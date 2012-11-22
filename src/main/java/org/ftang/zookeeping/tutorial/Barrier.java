package org.ftang.zookeeping.tutorial;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Random;

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

        cleanup(root, root + "-ready");

        // Create barrier node
        if (zk != null) {
            try {
                Stat s = zk.exists(root, false);
                if (s == null) {
                    zk.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
                zk.exists(root + "-ready", this);
            } catch (KeeperException e) {
                log.debug("Keeper exception when instantiating queue: " + e.toString());
            } catch (InterruptedException e) {
                log.debug("Interrupted exception");
            }
        }

        // My node name
        try {
            name = new String(InetAddress.getLocalHost().getCanonicalHostName().toString()) + port;
        } catch (UnknownHostException e) {
            log.error(e.toString());
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
        zk.create(root + "/" + name, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        while (true) {
            synchronized (mutex) {
                List<String> list = zk.getChildren(root, false);

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
                List<String> list = zk.getChildren(root, false);
                log.debug(String.format("size of %s is %s", root, list.size()));
                if (list.size() > 0) {
                    mutex.wait();
                } else {
                    return true;
                }
            }
        }
    }

    public static void barrierTest(String hostname, int number, int port) {
        String root = "/b1";
        String readyNode = root + "-ready";
        
        Barrier b = new Barrier(hostname, port, root, number);
        try{
            boolean flag = b.enter();
            log.debug("Entered barrier: " + number);
            if(!flag) log.debug("Error when entering the barrier");
        }  catch (KeeperException e){
            log.error("KeeperException", e);
        } catch (InterruptedException e){
            log.error("InterriptedException", e);
        }

        // Generate random integer
        Random rand = new Random();
        int r = rand.nextInt(100);
        // Loop for rand iterations
        for (int i = 0; i < r; i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {

            }
        }
        try{
            log.debug("Leaving barrier " + number);
            b.leave();
            if (zk.exists(readyNode, false) == null)
                zk.create(readyNode, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.close();
        } catch (KeeperException e){
            log.error("KeeperException", e);
        } catch (InterruptedException e){
            log.error("InterriptedException", e);
        }
        log.debug("Left barrier " + number);
    }

    private void cleanup(String root, String readyNode) {
        try {
            zk.delete(readyNode, 0);
            zk.delete(root, 0);
        } catch (KeeperException.NoNodeException e){
            // other node did the cleanup
        } catch (InterruptedException e){
            log.error("InterriptedException", e);
        } catch (KeeperException e){
            log.error("KeeperException", e);
        }
    }
}
