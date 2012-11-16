package org.ftang.zookeeping.tutorial;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class SyncPrimitive implements Watcher {

    private static Logger log = Logger.getLogger(SyncPrimitive.class);
    
    private static int TIMEOUT = 60000;
    static ZooKeeper zk = null;
    static Integer mutex;

    String root;

    SyncPrimitive(String address, int port) {
        if(zk == null){
            try {
                log.debug(String.format("Starting ZK: %s:%s", address, port));
                zk = new ZooKeeper(String.format("%s:%s", address, port), TIMEOUT, this);
                mutex = new Integer(-1);
                log.debug("Finished starting ZK: " + zk);
            } catch (IOException e) {
                log.error(e.toString());
                zk = null;
            }
        }
        //else mutex = new Integer(-1);
    }

    synchronized public void process(WatchedEvent event) {
        synchronized (mutex) {
            //log.debug("Process: " + event.getType());
            mutex.notify();
        }
    }

    public static void main(String args[]) throws InterruptedException {
        List<Thread> threads = new ArrayList();
        if (args[0].equals("qTest")) {
            threads.add(Queue.queueTest("localhost", 2187, true));
            threads.add(Queue.queueTest("localhost", 2188, false));
            //threads.add(queueTest("localhost", 2187, false));
        } else {
            for (int i = 0; i < 3; i++) {
                final int numThread = i;
                threads.add(new Thread() {
                    @Override
                    public void run() {
                        setName("BarrierThread " + numThread);
                        Barrier.barrierTest("localhost", numThread, 2187 + numThread);
                    }
                });
            }
        }
        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();
    }


    
}