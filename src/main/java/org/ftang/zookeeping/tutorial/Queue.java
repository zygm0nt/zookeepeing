package org.ftang.zookeeping.tutorial;

import com.google.common.base.Joiner;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Producer-Consumer queue
 */
public class Queue extends SyncPrimitive {

    private static Logger log = Logger.getLogger(Queue.class);
    /**
     * Constructor of producer-consumer queue
     *
     * @param address
     * @param name
     */
    Queue(String address, int port, String name) {
        super(address, port);
        this.root = name;
        // Create ZK node name
        if (zk != null) {
            try {
                Stat s = zk.exists(root, false);
                if (s == null) {
                    zk.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
            } catch (KeeperException e) {
                log.error("Keeper exception when instantiating queue: " + e.toString());
            } catch (InterruptedException e) {
                log.error("Interrupted exception");
            }
        }
    }

    /**
     * Add element to the queue.
     *
     * @param i
     * @return
     */

    boolean produce(int i) throws KeeperException, InterruptedException{
        ByteBuffer b = ByteBuffer.allocate(4);
        byte[] value;

        // Add child with value i
        b.putInt(i);
        value = b.array();
        zk.create(root + "/element", value, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        log.debug(String.format("Producing: %s/element", root));
        return true;
    }


    /**
     * Remove first element from the queue.
     *
     * @return
     * @throws org.apache.zookeeper.KeeperException
     * @throws InterruptedException
     */
    int consume() throws KeeperException, InterruptedException{
        int retvalue = -1;
        Stat stat = null;

        // Get the first element available
        while (true) {
            synchronized (mutex) {
                List<String> list = zk.getChildren(root, true);
                if (list.size() == 0) {
                    log.debug("Going to wait");
                    mutex.wait();
                } else {
                    String key = list.get(0).substring(7);
                    Integer min = new Integer(list.get(0).substring(7));
                    log.debug(Joiner.on(", ").join(list));
                    for(String s : list){
                        Integer tempValue = new Integer(s.substring(7));
                        log.debug("Temporary value: " + tempValue);
                        //if(tempValue < min) min = tempValue;
                    }
                    log.debug("Temporary value: " + root + "/element" + key);
                    byte[] b = zk.getData(root + "/element" + key, false, stat);
                    log.debug("Deleting: " + root + "/element" + key);
                    zk.delete(root + "/element" + key, 0);
                    ByteBuffer buffer = ByteBuffer.wrap(b);
                    retvalue = buffer.getInt();

                    return retvalue;
                }
            }
        }
    }

    public static Thread queueTest(final String host, final int port, final boolean producer) {
        return new Thread() {
            public void run() {
                Queue q = new Queue(host, port, "/app1");

                log.debug("Input: " + host);
                int i;
                Integer max = new Integer(20);

                if (producer) {
                    log.debug("Producer");
                    for (i = 0; i < max; i++)
                        try{
                            q.produce(i);
                        } catch (KeeperException e){ }
                        catch (InterruptedException e){ }
                } else {
                    log.debug("Consumer");

                    for (i = 0; i < max; i++) {
                        try{
                            int r = q.consume();
                            log.debug("Item: " + r);
                        } catch (KeeperException e){
                            i--;
                        } catch (InterruptedException e){ }
                    }
                }
            }
        };
    }

}
