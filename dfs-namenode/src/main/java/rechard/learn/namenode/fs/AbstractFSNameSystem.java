package rechard.learn.namenode.fs;


import rechard.learn.namenode.exeception.NameNodeException;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Rechard
 **/
public abstract class AbstractFSNameSystem implements FSNameSystem {

    private FSDirectory fsDirectory;
    private OperationLog operationLog;
    private String storeDir; //存储的路径肯定要知道，放在这个类里很合适
    private AtomicLong txIdGenerator = new AtomicLong();

    public AbstractFSNameSystem(String storeDir) {
        this.fsDirectory = new FSDirectory();
        this.operationLog = new OperationLog(storeDir);
    }

    @Override
    public void mkdir(String path, Map<String, String> attr) throws NameNodeException {
        this.fsDirectory.mkdir(path, attr);
        //刷盘
        EditLog editLog = new EditLog();
        txIdGenerator.incrementAndGet();
        editLog.setTxId(txIdGenerator.incrementAndGet());
        editLog.setPath(path);
        editLog.setOpType(NameNodeConstant.MKDIR);
        editLog.setAttr(attr);
        try {
            this.operationLog.saveLog(editLog);
        } catch (IOException e) {
            throw new NameNodeException(e);
        }
    }

    @Override
    public boolean createFile(String filename, Map<String, String> attr) throws NameNodeException {
        return this.fsDirectory.createFile(filename, attr);
    }

    @Override
    public boolean deleteFile(String filename) throws NameNodeException {
        return this.fsDirectory.deleteFile(filename);
    }

    public void recoveryNamespace() {

    }
}
