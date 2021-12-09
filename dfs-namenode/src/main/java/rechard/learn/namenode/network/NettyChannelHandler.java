package rechard.learn.namenode.network;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;
import rechard.learn.dfs.common.network.Packet;
import rechard.learn.dfs.common.utils.DefaultScheduler;
import rechard.learn.namenode.processor.handler.NameNodeApis;

/**
 * @author Rechard
 **/
@ChannelHandler.Sharable
@Slf4j
public class NettyChannelHandler extends ChannelInboundHandlerAdapter {
    protected DefaultScheduler scheduler;
    protected NameNodeApis apis;

    public NettyChannelHandler(DefaultScheduler scheduler, NameNodeApis nameNodeApis) {
        this.scheduler = scheduler;
        this.apis = nameNodeApis;
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
        log.info("{} connnect to me success", ctx.channel().remoteAddress());
        //需要验证ctx是否已经验证过，如果没有就要断开连接
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        // super.channelRead(ctx, msg);
        // System.out.println(ctx.channel().remoteAddress() + "发送消息：" + msg);
        //收到消息需要处理
        //处理不能在这里面做处理，参考rocketmq的做法，定义1个threadpoolexecutor来处理
        //需要将消息先封装然后交给processor来处理
        //rocketmq的消息处理是pair<消息,>,就是将某些消息
        if (msg instanceof Packet) {
            Packet packet = (Packet) msg;

            if (this.getThreadPoolExecutor() != null) {
                this.getThreadPoolExecutor().scheduleOnce("process msgType=" + packet.getMsgType(), () -> {
                    handle(ctx, packet);
                });
            } else {
                handle(ctx, packet);
            }
        }
    }

    public DefaultScheduler getThreadPoolExecutor() {
        return this.scheduler;
    }

    public void handle(ChannelHandlerContext ctx, Packet packet) {
        apis.process(packet, ctx);
    }
}
