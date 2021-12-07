package rechard.learn.namenode.network;

import com.ruyuan.dfs.model.namenode.NameNodeAwareRequest;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import rechard.learn.namenode.config.NameNodeConfig;
import rechard.learn.namenode.constant.MsgType;
import rechard.learn.namenode.processor.handler.NameNodeApis;
import rechard.learn.namenode.protocol.Packet;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;

/**
 * @author Rechard
 **/
@Slf4j
public class NettyClientChannelHandler extends NettyChannelHandler {

    public NettyClientChannelHandler(ExecutorService executorService, NameNodeApis nameNodeApis) {
        super(executorService, nameNodeApis);
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.info("client connect to server{}", ctx.channel().remoteAddress());
        NameNodeConfig nameNodeConfig = apis.getNameNodeConfig();
        super.channelActive(ctx);
        InetSocketAddress socketAddress = (InetSocketAddress) ctx.channel().localAddress();
       /* NameNodeInfo selfInfo=NameNodeInfo.newBuilder()
                .setHostname(socketAddress.getHostName())
                .setNioPort(nameNodeConfig.getPort())
                .setNodeId(nameNodeConfig.getNameNodeId())
                .build();*/
        NameNodeAwareRequest selfInfo = NameNodeAwareRequest.newBuilder()
                .setServer(socketAddress.getHostName() + ":" + nameNodeConfig.getPort())
                .setIsClient(true) //client上报
                .setNameNodeId(nameNodeConfig.getNameNodeId())
                .build();
        Packet packet = Packet.builder().msgType(MsgType.NAME_NODE_PEER_AWARE.code())
                .body(selfInfo.toByteArray())
                .build();
        ctx.channel().writeAndFlush(packet);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
    }
}
