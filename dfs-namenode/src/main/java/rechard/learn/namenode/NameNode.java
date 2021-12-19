package rechard.learn.namenode;

import cn.hutool.core.io.FileUtil;
import lombok.extern.slf4j.Slf4j;
import rechard.learn.dfs.common.exeception.NameNodeException;
import rechard.learn.dfs.common.fs.*;
import rechard.learn.dfs.common.utils.DefaultScheduler;
import rechard.learn.namenode.config.NameNodeConfig;
import rechard.learn.namenode.manager.ControllerManager;
import rechard.learn.namenode.network.NettyClient;
import rechard.learn.namenode.network.NettyServer;
import rechard.learn.namenode.peer.PeerNode;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author Rechard
 **/
@Slf4j
public class NameNode {
    private NettyServer nettyServer;
    private NameNodeConfig nameNodeConfig;
    private ControllerManager controllerManager;
    private FSDirectory fsDirectory;
    private FSNameSystem fsNameSystem;
    // private OperationLog operationLog;
    private DefaultScheduler scheduler;

    public NameNode(NameNodeConfig nameNodeConfig) {
        this.nameNodeConfig = nameNodeConfig;
    }

    public void start() throws NameNodeException, IOException {
        scheduler = new DefaultScheduler("namenode-thread-");
        recoverFileNameSystem();

        //1.启动1个nettyserver
        controllerManager = new ControllerManager(nameNodeConfig);
        nettyServer = new NettyServer(nameNodeConfig, controllerManager, fsDirectory, scheduler);
        nettyServer.start();

        //3.此时去连接其他的namenode
        //问题1：如果先启动id大的去连接其他还没有启动成功的服务器一定会报错怎么办？
        //做成不断的重试,所以现在要做的就是不断去重试
        //问题2: 如果还没有选举成功，则不应该对外服务
        //留待后面
        PeerNode[] peerNodes = nameNodeConfig.getPeerNodes();
        //ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors() * 2);

        for (final PeerNode peerNode : peerNodes) {
            //只允许id大的向小的server连接
            if (peerNode.getId() != nameNodeConfig.getNameNodeId()
                    && nameNodeConfig.getNameNodeId() > peerNode.getId()) {
                log.info("connectting to namenode-{}:{}", peerNode.getHost(), peerNode.getPort());
                NettyClient nettyClient = new NettyClient(scheduler, nameNodeConfig, controllerManager, fsDirectory);
                nettyClient.connectAsync(peerNode.getHost(), peerNode.getPort());
                //什么时候开始选举？当连接成功后，就向服务器发送自己得信息，如果接收到得服务信息和配置一致，就发起选举
                //首先要先将消息的protol进行定义好
                //protol是什么呢
                //len+headerlen+header+bodylen+body
            }
        }
    }

    /**
     * 从磁盘文件里恢复
     * fsimg/txid.fsi  这个是fsimg的保存的文件
     * log/001000.log
     * log/002000.log
     * log/003000.log
     * <p>
     * 考虑几种情况
     * 1. 正常恢复
     * 2. 异常恢复
     * 3. 第一次启动
     *
     * @return
     * @throws NameNodeException
     */
    private void recoverFileNameSystem() throws NameNodeException, IOException {
        //1.扫描所有的fsimage文件
        File fsImgDir = nameNodeConfig.getFsimgDir();
        //fsimage命名规则是
        //txid.fsi
        List<File> files = FileUtil.loopFiles(fsImgDir, 1, file -> {
            return file.isFile() && file.getName().endsWith(".fsi");
        });
        FsImage fsImage = null;
        File lastValidFsFile = null;
        int index = 0;
        if (files != null) {
            //逆序排列
            files.sort(Comparator.reverseOrder());
            for (index = 0; index < files.size(); index++) {
                try {
                    lastValidFsFile = files.get(index);
                    log.info("parse {} into fsimage", lastValidFsFile.getName());
                    fsImage = FsImage.parseFile(lastValidFsFile);
                } catch (IOException e) {
                    log.warn("{} can not parse into fsimage with error {}", lastValidFsFile.getName(), e);
                }
                //解析靠的是protobuf序列化,能序列出来表示可以解析成功
                if (fsImage != null)
                    break;
            }
        }
        if (fsImage == null && files != null) {
            //所有文件没法启动成功
            throw new NameNodeException("all fsimage file can not parsed,consider delete these files and restart" +
                    "if do so all history data LOST!!!");
        }
        if (files.isEmpty() && index == 0 && fsImage == null) {
            //第一次进来,什么都不处理
            log.info("first start");
        }
        Node root = fsImage.getRoot();

        this.fsDirectory = new FSDirectory();
        fsDirectory.setRoot(root);
        long txid = fsImage.getMaxTxId();
        //2.检查editlog是否完整，如果完整就合并到fsimage里
        //由于editlog是异步刷盘，此时有可能发生
        //异常退出的时候刷盘还没完成
        File editLogDir = nameNodeConfig.getEditLogDir();
        //editog命名规则是
        //txid.log
        List<File> editLogFiles = FileUtil.loopFiles(fsImgDir, 1, file -> {
            String logFileName = file.getName();
            long startTxid = Long.parseLong(logFileName);
            //logfile的txid大于fsimg的txid
            return file.isFile() && logFileName.endsWith(".log") && startTxid >= txid;
        });
        editLogFiles.sort(Comparator.naturalOrder());
        List<OperationLog> allOperationLog = new ArrayList<>();
        OperationLog operationLog = null;
        for (File file : editLogFiles) {
            operationLog = new OperationLog(file);
            allOperationLog.add(operationLog);
            if (!operationLog.checkAndreplay(fsDirectory, txid)) {
                log.info("editlog file {} replay not all pass!!", file.getName());
                break;
            }
        }
        ;

        if (operationLog == null) {
            //第一次启动
            File firstLogFile = new File(this.nameNodeConfig.getEditLogDir() + "/" + 0 + ".log");
            if (!firstLogFile.exists()) {
                FileUtil.mkdir(firstLogFile.getParent());
            }
            operationLog = new OperationLog(firstLogFile);
        }
        this.fsNameSystem = new AsyncLogNameSystemIml(new FSNameSystemIml(this.fsDirectory), this.scheduler);
        ((AsyncLogNameSystemIml) fsNameSystem).setBeginTxId(operationLog.getCurrentTxid());
    }

    public void shutdonw() {
        //todo
        System.out.println("shutdown...");
    }
}
