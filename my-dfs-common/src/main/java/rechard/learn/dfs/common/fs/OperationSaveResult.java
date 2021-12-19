package rechard.learn.dfs.common.fs;

/**
 * 在创建的过程中碰到文件大于1m的情况下，就需要创建另个文件
 * 记录刷盘的结果
 *
 * @author Rechard
 **/
public class OperationSaveResult {

    public static final int PUT_OK = 0;
    public static final int CREATE_NEW_FILE_AND_PUT_OK = 1;

    private OperationLog operationLog;
    private int state;


    public OperationLog getOperationLog() {
        return operationLog;
    }

    public void setOperationLog(OperationLog operationLog) {
        this.operationLog = operationLog;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }
}
