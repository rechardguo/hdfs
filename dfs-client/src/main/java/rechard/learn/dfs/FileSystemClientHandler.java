package rechard.learn.dfs;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import rechard.learn.dfs.callback.MessageCallback;
import rechard.learn.dfs.callback.Response;
import rechard.learn.dfs.common.constant.MsgType;
import rechard.learn.dfs.common.network.Packet;

import java.util.Map;


public class FileSystemClientHandler extends ChannelInboundHandlerAdapter {
    private MessageCallback callback;

    public FileSystemClientHandler(MessageCallback callback) {
        this.callback = callback;
    }

    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        Packet packet = (Packet) msg;
        int type = packet.getMsgType();
        MsgType msgType = MsgType.get(type);
        //处理mkdir
        if (msgType == MsgType.FS_OP_MKDIR_RESPONSE) {
            Map header = packet.getHeader();
            //回调接口
            this.callback.handle(new Response(type, header));
        }
    }

    //channel注册，注册到selector
    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        super.channelWritabilityChanged(ctx);
    }

    //通道就绪,发送auth 认证
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}