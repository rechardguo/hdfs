package rechard.learn.namenode;

import rechard.learn.namenode.config.NameNodeConfig;
import rechard.learn.namenode.network.NettyClient;
import rechard.learn.namenode.network.NettyServer;
import rechard.learn.namenode.peer.PeerNode;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author Rechard
 **/
public class NameNode {

    public NettyServer nettyServer;
    public NameNodeConfig nameNodeConfig;

    public NameNode(NameNodeConfig nameNodeConfig) {
        this.nameNodeConfig = nameNodeConfig;
    }

    public void start() {
        //1.启动1个nettyserver
        nettyServer = new NettyServer(nameNodeConfig);
        nettyServer.start();

        //2.此时去连接其他的namenode
        //问题1：如果先启动id大的去连接其他还没有启动成功的服务器一定会报错怎么办？
        //做成不断的重试,所以现在要做的就是不断去重试
        //问题2: 如果还没有选举成功，则不应该对外服务
        PeerNode[] peerNodes = nameNodeConfig.getPeerNodes();
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors() * 2);

        for (final PeerNode peerNode : peerNodes) {
            //只允许id大的向小的server连接
            if (peerNode.getId() != nameNodeConfig.getNameNodeId()
                    && nameNodeConfig.getNameNodeId() > peerNode.getId()) {
                System.out.println(String.format("connectting to namenode-%s:%d", peerNode.getHost(), peerNode.getPort()));
                new NettyClient(scheduledExecutorService)
                        .connectAsync(peerNode.getHost(), peerNode.getPort());
            }
        }

    }

    public void shutdonw() {
        //todo
        System.out.println("shutdown...");
    }
}
