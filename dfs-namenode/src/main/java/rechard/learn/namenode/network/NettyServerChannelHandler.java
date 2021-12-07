package rechard.learn.namenode.network;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import rechard.learn.namenode.processor.handler.NameNodeApis;

import java.util.concurrent.ExecutorService;

/**
 * @author Rechard
 **/
@Slf4j
public class NettyServerChannelHandler extends NettyChannelHandler {

    public NettyServerChannelHandler(ExecutorService executorService, NameNodeApis nameNodeApis) {
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
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
    }
}
