package rechard.learn.namenode.sync;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author Rechard
 **/
public class SyncTask {
    public static final int STATE_COMPLETE = 1;
    public static final int STATE_SYNC_TIMEOUT = 2;
    public static final int STATE_SYNC_FAIL = 3;

    private long txid;
    private int state;//最终的任务完成的state是什么
    private CountDownLatch countDownLatch;

    public SyncTask(long txid) {
        this.txid = txid;
        this.countDownLatch = new CountDownLatch(1);
    }

    public long getTxid() {
        return txid;
    }

    public boolean await(int waitMill) throws InterruptedException {
        return countDownLatch.await(waitMill, TimeUnit.MILLISECONDS);
    }

    public boolean isDone() {
        return countDownLatch.getCount() == 0;
    }

    public int getState() {
        return state;
    }

    /**
     * 改变状态并唤醒等待在该任务的线程
     *
     * @param state
     * @throws InterruptedException
     */
    public void setStateAndNotify(int state) throws InterruptedException {
        this.state = state;
        countDownLatch.countDown();
        synchronized (this) {
            notify();
        }
    }

    //等待结束
    public void waitOnCompleted() throws InterruptedException {
        synchronized (this) {
            wait();
        }
    }
}
