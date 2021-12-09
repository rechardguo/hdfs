package rechard.learn.namenode.processor.handler;

import com.google.protobuf.InvalidProtocolBufferException;
import com.ruyuan.dfs.model.namenode.NameNodeAwareRequest;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import rechard.learn.dfs.common.constant.MsgType;
import rechard.learn.dfs.common.network.Packet;
import rechard.learn.namenode.config.NameNodeConfig;
import rechard.learn.namenode.fs.FSDirectory;
import rechard.learn.namenode.fs.FsImage;
import rechard.learn.namenode.manager.ControllerManager;
import rechard.learn.namenode.peer.PeerNode;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import static rechard.learn.dfs.common.constant.MsgType.BACK_NODE_AUTH_RESPONSE;
import static rechard.learn.dfs.common.constant.MsgType.BACK_NODE_FETCH_FSIMAGE_RESPONSE;

/**
 * @author Rechard
 **/
@Slf4j
public class NameNodeApis {

    private ControllerManager controllerManager;
    private FSDirectory fsDirectory;
    private NameNodeConfig nameNodeConfig;

    public NameNodeApis(ControllerManager controllerManager, NameNodeConfig nameNodeConfig, FSDirectory fsDirectory) {
        this.controllerManager = controllerManager;
        this.nameNodeConfig = nameNodeConfig;
        this.fsDirectory = fsDirectory;
    }

    public NameNodeConfig getNameNodeConfig() {
        return nameNodeConfig;
    }

    public void process(Packet p, ChannelHandlerContext ctx) {
        MsgType msgType = MsgType.get(p.getMsgType());
        /*Method[] declaredMethods = this.getClass().getDeclaredMethods();
        for (Method m : declaredMethods) {
            PacketHandler ph = m.getAnnotation(PacketHandler.class);
            if (ph != null && ph.type() == msgType) {
                try {
                    m.invoke(this, p, ctx);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }*/
        switch (msgType) {
            case NAME_NODE_PEER_AWARE:
                processNamenodeAwares(p, ctx);
            case BACK_NODE_AUTH_REQUEST:
                handleAuth(p, ctx);
            case BACK_NODE_FETCH_FSIMAGE_REQUEST:
                handleImageFetch(p, ctx);

        }
    }

    private void handleImageFetch(Packet p, ChannelHandlerContext ctx) {
        FsImage fsImage = fsDirectory.getFsImage();
        Packet packet = Packet.builder().msgType(BACK_NODE_FETCH_FSIMAGE_RESPONSE.code())
                //  .body()
                .build();
        ctx.channel().writeAndFlush(packet);
    }

    private void handleAuth(Packet p, ChannelHandlerContext ctx) {
        Map header = p.getHeader();
        Map<String, String> map = new HashMap<>();
        if (this.nameNodeConfig.getPass().equals(header.get("pass"))) {
            map.put("result", "ok");
        } else {
            map.put("result", "fail");
        }
        Packet packet = Packet.builder().msgType(BACK_NODE_AUTH_RESPONSE.code())
                .header(map)
                .build();
        ctx.channel().writeAndFlush(packet);
    }

    //@PacketHandler(type = MsgType.NAME_NODE_PEER_AWARE)
    public void processNamenodeAwares(Packet p, ChannelHandlerContext ctx) {
        byte[] bodyBytes = p.getBody();
        try {
            NameNodeAwareRequest recvNameNodeInfo = NameNodeAwareRequest.parseFrom(bodyBytes);

            log.info("receive namenode peer name_node_peer_aware msg:{}", recvNameNodeInfo);
            String server = recvNameNodeInfo.getServer();
            String[] serverInfo = server.split(":");
            PeerNode peerNode = new PeerNode(serverInfo[0], Integer.parseInt(serverInfo[1]), recvNameNodeInfo.getNameNodeId());
            controllerManager.addPeerNode(peerNode);
            //将自己上报给对方
            if (recvNameNodeInfo.getIsClient()) {
                InetSocketAddress socketAddress = (InetSocketAddress) ctx.channel().localAddress();
                NameNodeAwareRequest selfInfo = NameNodeAwareRequest.newBuilder()
                        .setServer(socketAddress.getHostName() + ":" + nameNodeConfig.getPort())
                        .setIsClient(false) // server端的上报
                        .setNameNodeId(nameNodeConfig.getNameNodeId())
                        .build();
                Packet packet = Packet.builder().msgType(MsgType.NAME_NODE_PEER_AWARE.code())
                        .body(selfInfo.toByteArray())
                        .build();
                ctx.channel().writeAndFlush(packet);
            }
            //controllerManager.startElection();
        } catch (InvalidProtocolBufferException e) {
            //如果解析错误
            //e.printStackTrace();
            log.error("error in parsing packet", e);
        }

    }

}
