package rechard.learn.namenode.manager;

import rechard.learn.namenode.config.NameNodeConfig;
import rechard.learn.namenode.peer.PeerNode;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 用于控制nameNode之间的管理
 *
 * @author Rechard
 **/
public class ControllerManager {

    private NameNodeConfig nameNodeConfig;

    public ControllerManager(NameNodeConfig nameNodeConfig) {
        this.nameNodeConfig = nameNodeConfig;
    }

    //接收到的namenode结点
    private java.util.List<PeerNode> peerNodes = new ArrayList<>();
    //是否选举已经完成，默认是false
    private AtomicBoolean electionDone = new AtomicBoolean(false);

    public void addPeerNode(PeerNode node) {
        this.peerNodes.add(node);
    }

    public void startElection() {
        if (electionDone.get()) return;
        //config peerNode
        PeerNode[] nameNodes = nameNodeConfig.getPeerNodes();
        //在一个循环中进行选举
        while (!electionDone.get()) {
            //不断发送vote给其他的namenode结点

        }
    }
}
