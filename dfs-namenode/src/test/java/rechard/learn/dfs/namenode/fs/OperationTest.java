package rechard.learn.dfs.namenode.fs;

import cn.hutool.core.io.FileUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import rechard.learn.namenode.fs.EditLog;
import rechard.learn.namenode.fs.OperationLog;

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
        OperationLog log = new OperationLog(storeDir);
        long startTxId = 1000;
        log.createEditLog(startTxId, 100);//100k
        for (int i = 0; i < 10; i++) {
            EditLog editLog = new EditLog();
            editLog.setTxId(startTxId + i);
            editLog.setPath("/a/b/c");
            editLog.setOpType(NameNodeConstant.MKDIR);
            Map attr = new HashMap();
            editLog.setAttr(attr);
            log.saveLog(editLog);
        }
        //检查文件数
        OperationLog loadLog = OperationLog.loadLogFile(storeDir + File.separator + startTxId);

    }


}
