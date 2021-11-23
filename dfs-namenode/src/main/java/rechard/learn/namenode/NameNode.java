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
        //留待后面
        PeerNode[] peerNodes = nameNodeConfig.getPeerNodes();
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors() * 2);

        for (final PeerNode peerNode : peerNodes) {
            //只允许id大的向小的server连接
            if (peerNode.getId() != nameNodeConfig.getNameNodeId()
                    && nameNodeConfig.getNameNodeId() > peerNode.getId()) {
                System.out.println(String.format("connectting to namenode-%s:%d", peerNode.getHost(), peerNode.getPort()));

                NettyClient nettyClient = new NettyClient(scheduledExecutorService);
                nettyClient.connectAsync(peerNode.getHost(), peerNode.getPort());
                //什么时候开始选举？当连接成功后，就向服务器发送自己得信息，如果接收到得服务信息和配置一致，就发起选举
                //首先要先将消息的protol进行定义好
                //protol是什么呢
                //len+headerlen+header+bodylen+body
            }
        }

    }

    public void shutdonw() {
        //todo
        System.out.println("shutdown...");
    }
}
