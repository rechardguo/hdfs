package rechard.learn.backnode;

import lombok.extern.slf4j.Slf4j;
import rechard.learn.backnode.config.BackupNodeConfig;
import rechard.learn.backnode.network.NameNodeClient;
import rechard.learn.dfs.common.utils.DefaultScheduler;

/**
 * @author Rechard
 **/
@Slf4j
public class BackupNode {
    private NameNodeClient nettyClient;
    private DefaultScheduler scheduler;
    private BackupNodeConfig backupNodeConfig;

    public BackupNode(BackupNodeConfig backupNodeConfig) {
        this.backupNodeConfig = backupNodeConfig;
    }

    public void start() {
        this.scheduler = new DefaultScheduler("backup-namenode-");
        //1.启动backup node netty server
        //nettyServer = new NettyServer(backupNodeConfig);
        //nettyServer.start();

        //2.向master连接
        nettyClient = new NameNodeClient(backupNodeConfig, scheduler);
        nettyClient.connectMaster();
    }

    public void shutdonw() {
        //todo
        System.out.println("shutdown...");
    }
}
