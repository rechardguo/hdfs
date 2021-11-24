package rechard.learn.namenode.network;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;
import rechard.learn.namenode.constant.MsgType;
import rechard.learn.namenode.protocol.Packet;

/**
 * @author Rechard
 **/
@Slf4j
public class NettyClientChannelHandler extends ChannelInboundHandlerAdapter {
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
        log.info("客户端连接{}成功", ctx.channel().remoteAddress());
        super.channelActive(ctx);
        //将自己的消息上报给对方
        //ctx.channel().writeAndFlush("我是客户端");
        Packet packet = Packet.builder().msgType(MsgType.NAME_NODE_PEER_AWARE.code())
                .body("我是客户端".getBytes())
                .build();
        ctx.channel().writeAndFlush(packet);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        super.channelRead(ctx, msg);
    }
}
