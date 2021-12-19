package rechard.learn.dfs.namenode.fs;

import cn.hutool.core.io.FileUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import rechard.learn.dfs.common.constant.NameNodeConstant;
import rechard.learn.dfs.common.fs.OperationLog;
import rechard.learn.dfs.common.fs.OperationLogItem;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 操作日志testcase
 *
 * @author Rechard
 **/
public class OperationTest {

    private static String storeDir = System.getProperty("user.dir") + "/../" + File.separator + "test-store";

    @BeforeClass
    public static void cleanStore() {
        File dir = new File(storeDir);
        if (dir.exists()) {
            for (File f : dir.listFiles())
                FileUtil.del(f);
        } else {
            FileUtil.mkdir(dir);
        }
    }

    @Test
    public void testAppendLog() throws IOException {
        long startTxId = 1000;
        File logFile = new File(storeDir + "/" + startTxId + ".log");
        OperationLog log = new OperationLog(logFile);

        log.createOperationLog(startTxId, 100);//100k
        for (int i = 0; i < 10; i++) {
            OperationLogItem operationLogItem = new OperationLogItem();
            operationLogItem.setTxId(startTxId + i);
            operationLogItem.setPath("/a/b/c");
            operationLogItem.setOpType(NameNodeConstant.MKDIR);
            Map attr = new HashMap();
            operationLogItem.setAttr(attr);
            log.saveLog(operationLogItem);
        }
        //检查文件数
        //OperationLog loadLog = OperationLog.loadLogFile(storeDir + File.separator + startTxId);


    }


}
