package rechard.learn.dfs.common.fs;

import rechard.learn.dfs.common.exeception.NameNodeException;
import rechard.learn.dfs.common.utils.DefaultScheduler;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static rechard.learn.dfs.common.constant.NameNodeConstant.*;

/**
 * 异步刷盘
 *
 * @author Rechard
 **/
public class AsyncLogNameSystemIml implements FSNameSystem, Runnable {

    private FSNameSystem fsNameSystem;
    private AtomicReference<OperationLog> curOperationLog; //当前的operationLog
    // 读多写少的场景
    private CopyOnWriteArrayList<OperationLog> allOperationLogs;
    private AtomicLong txIdGenerator = new AtomicLong();
    private DefaultScheduler scheduler;

    public AsyncLogNameSystemIml(FSNameSystem fsNameSystem, DefaultScheduler scheduler) {
        this.fsNameSystem = fsNameSystem;
        this.allOperationLogs = new CopyOnWriteArrayList<>();
        this.curOperationLog = new AtomicReference();
        //this.operationLog = operationLog;
        this.scheduler = scheduler;
        //实现异步刷盘
        //这里有个问题怎么知道刷盘已经完成了呢
        this.scheduler.schedule("editlog-flush-task", this, 0, 10, TimeUnit.MILLISECONDS);
    }

    public void addOpreationLog(OperationLog operationLog) {
        this.allOperationLogs.add(operationLog);
    }

    @Override
    public void mkdir(String path, Map<String, String> attr) throws NameNodeException {
        this.fsNameSystem.mkdir(path, attr);
        logAsync(MKDIR, path, attr);
    }

    @Override
    public boolean createFile(String path, Map<String, String> attr) throws NameNodeException {
        boolean result = this.fsNameSystem.createFile(path, attr);
        logAsync(CREATE_FILE, path, attr);
        return result;
    }

    @Override
    public boolean deleteFile(String path) throws NameNodeException {
        boolean result = this.fsNameSystem.deleteFile(path);
        logAsync(DELETE_FILE, path, null);
        return result;
    }

    private void logAsync(int operationType, String path, Map<String, String> attr) {
        //记录日志
        OperationLogItem operationLogItem = new OperationLogItem();
        operationLogItem.setTxId(txIdGenerator.incrementAndGet());
        operationLogItem.setPath(path);
        operationLogItem.setOpType(operationType);
        operationLogItem.setAttr(attr);
        try {
            OperationSaveResult operationSaveResult = this.curOperationLog.get().saveLog(operationLogItem);
            //如果建立了一个新的editlog
            if (operationSaveResult.getState() == OperationSaveResult.CREATE_NEW_FILE_AND_PUT_OK) {
                allOperationLogs.add(operationSaveResult.getOperationLog());
                this.curOperationLog.set(operationSaveResult.getOperationLog());
            }
        } catch (IOException e) {
            //todo
            e.printStackTrace();
        }
    }

    //设置开始的txid
    public void setBeginTxId(long txid) {
        this.txIdGenerator.set(txid);
    }

    @Override
    public void run() {
        //不断的去将operationlog刷入的硬盘上

    }
}
