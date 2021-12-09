package rechard.learn.namenode.network;

import com.ruyuan.dfs.model.namenode.NameNodeAwareRequest;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import rechard.learn.dfs.common.constant.MsgType;
import rechard.learn.dfs.common.network.Packet;
import rechard.learn.dfs.common.utils.DefaultScheduler;
import rechard.learn.namenode.config.NameNodeConfig;
import rechard.learn.namenode.processor.handler.NameNodeApis;

import java.net.InetSocketAddress;

/**
 * @author Rechard
 **/
@Slf4j
public class NettyClientChannelHandler extends NettyChannelHandler {

    public NettyClientChannelHandler(DefaultScheduler scheduler, NameNodeApis nameNodeApis) {
        super(scheduler, nameNodeApis);
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
