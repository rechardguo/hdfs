package rechard.learn.namenode.sync;

import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.LinkedList;

/**
 * 消息同步器
 *
 * @author Rechard
 **/
@Slf4j
public class Synctor extends Thread {
    //这样做的好处是无锁
    private LinkedList<SyncTask> readRequest = new LinkedList();
    private LinkedList<SyncTask> writeRequest = new LinkedList();
    // backnode的txid
    private long backNodetxid;

    @Override
    public void run() {
        while (true) {
            try {
                sleep(10);
                processAndWaitReadRequest();
                swap();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void processAndWaitReadRequest() {
        Iterator<SyncTask> it = readRequest.iterator();
        while (it.hasNext()) {
            SyncTask task = it.next();
            if (task.getTxid() > backNodetxid) {
                try {
                    //没好的话，需要等待
                    task.await(200);
                } catch (InterruptedException e) {
                    //e.printStackTrace();
                    log.error("error in sync task txid={},error is {}", task.getTxid(), e);
                }
            }
        }
    }

    public SyncTaskFuture addTask(SyncTask task) {
        this.writeRequest.add(task);
        return new SyncTaskFuture(task);
    }

    private void swap() {
        LinkedList tmp = this.writeRequest;
        this.writeRequest = readRequest;
        this.readRequest = tmp;
    }

}
