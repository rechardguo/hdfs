package rechard.learn.namenode.sync;

import lombok.extern.slf4j.Slf4j;

/**
 * @author Rechard
 **/
@Slf4j
public class SyncTaskFuture {

    private SyncTask task;

    public SyncTaskFuture(SyncTask task) {
        this.task = task;
    }

    public int get() {
        //需要等待消息是否返回成功
        if (!task.isDone()) {
            try {
                task.waitOnCompleted();
            } catch (InterruptedException e) {
                log.error("error in wait backup node sync result");
                return SyncTask.STATE_SYNC_FAIL;
                //e.printStackTrace();
            }
        }
        return task.getState();
    }
}
