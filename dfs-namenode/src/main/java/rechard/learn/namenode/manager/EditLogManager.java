package rechard.learn.namenode.manager;

import cn.hutool.core.io.FileUtil;
import rechard.learn.dfs.common.fs.OperationLog;
import rechard.learn.namenode.config.NameNodeConfig;

import java.io.File;
import java.io.IOException;

/**
 * 负责记录日志操作日志
 * <p>
 * 如何记录log呢？参考rocketmq的做法
 * <p>
 * 1.记录操作日志
 * 2.同步backnode
 *
 * @author Rechard
 **/
public class EditLogManager {

    private NameNodeConfig nameNodeConfig;
    private long curTxId;
    private OperationLog log;

    public EditLogManager(NameNodeConfig nameNodeConfig) {
        this.nameNodeConfig = nameNodeConfig;


    }

    public void scanAllFiles() throws IOException {
        //从本地的路径里扫描得到所有的log
        File editLogDir = this.nameNodeConfig.getEditLogDir();
        java.util.List files = FileUtil.listFileNames(editLogDir.getPath());
        if (files != null) {

        }

    }

    public void create() throws IOException {
        log.createOperationLog(curTxId, 100);//100k
    }

}
