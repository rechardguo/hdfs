package rechard.learn.namenode.network;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;
import rechard.learn.namenode.processor.handler.NameNodeApis;
import rechard.learn.namenode.protocol.Packet;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author Rechard
 **/
@ChannelHandler.Sharable
@Slf4j
public class NettyServerChannelHandler extends ChannelInboundHandlerAdapter {
    private ThreadPoolExecutor threadPoolExecutor;
    private NameNodeApis apis = new NameNodeApis();

    public NettyServerChannelHandler() {

    }

    public NettyServerChannelHandler(ThreadPoolExecutor threadPoolExecutor) {
        this.threadPoolExecutor = threadPoolExecutor;
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
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        super.channelRead(ctx, msg);
        // System.out.println(ctx.channel().remoteAddress() + "发送消息：" + msg);
        //收到消息需要处理
        //处理不能在这里面做处理，参考rocketmq的做法，定义1个threadpoolexecutor来处理
        //需要将消息先封装然后交给processor来处理
        //rocketmq的消息处理是pair<消息,>,就是将某些消息
        if (msg instanceof Packet) {
            Packet packet = (Packet) msg;
            log.info(packet.toString());
            if (this.getThreadPoolExecutor() != null) {
                this.getThreadPoolExecutor().submit(() -> {
                    handle(ctx, packet);
                });
            } else {
                handle(ctx, packet);
            }
        }
    }

    public ThreadPoolExecutor getThreadPoolExecutor() {
        return this.threadPoolExecutor;
    }

    public void handle(ChannelHandlerContext ctx, Packet packet) {
        apis.process(packet, ctx);
    }
}
