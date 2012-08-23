package org.ftang.zookeeping;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.omg.CORBA.TIMEOUT;

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
            threads.add(queueTest("localhost", 2187, true));
            threads.add(queueTest("localhost", 2188, false));
            //threads.add(queueTest("localhost", 2187, false));
        } else {
            for (int i = 0; i < 3; i++) {
                final int numThread = i;
                threads.add(new Thread() {
                    @Override
                    public void run() {
                        setName("BarrierThread " + numThread);
                        barrierTest("localhost", numThread, 2187 + numThread);
                    }
                });
            }
        }
        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();
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

    public static void barrierTest(String hostname, int number, int port) {
        Barrier b = new Barrier(hostname, port, "/b1", number);
        try{
            boolean flag = b.enter();
            log.debug("Entered barrier: " + number);
            if(!flag) log.debug("Error when entering the barrier");
        } catch (KeeperException e){

        } catch (InterruptedException e){

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
            b.leave();
            zk.close();
        } catch (KeeperException e){

        } catch (InterruptedException e){

        }
        log.debug("Left barrier");
    }
}