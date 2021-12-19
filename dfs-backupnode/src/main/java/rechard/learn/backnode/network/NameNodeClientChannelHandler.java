package rechard.learn.backnode.network;

import com.ruyuan.dfs.model.client.AuthenticateInfoRequest;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;
import rechard.learn.backnode.config.BackupNodeConfig;
import rechard.learn.dfs.common.constant.MsgType;
import rechard.learn.dfs.common.network.Packet;
import rechard.learn.dfs.common.utils.DefaultScheduler;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static rechard.learn.dfs.common.constant.MsgType.BACK_NODE_AUTH_REQUEST;
import static rechard.learn.dfs.common.constant.MsgType.BACK_NODE_FETCH_FSIMAGE_REQUEST;
import static rechard.learn.dfs.common.constant.NameNodeConstant.FETCH_EDITLOG;
import static rechard.learn.dfs.common.constant.NameNodeConstant.FETCH_FSIMAGE;

/**
 * @author Rechard
 **/
@ChannelHandler.Sharable
@Slf4j
public class NameNodeClientChannelHandler extends ChannelInboundHandlerAdapter {
    private BackupNodeConfig backupNodeConfig;
    private DefaultScheduler scheduler;
    private static final int AUTH_INIT = 0;
    private static final int AUTH_SUCCESS = 1;
    private static final int AUTH_FAIL = 2;
    private volatile int authStatus = AUTH_INIT;

    public NameNodeClientChannelHandler(BackupNodeConfig backupNodeConfig, DefaultScheduler scheduler) {
        this.backupNodeConfig = backupNodeConfig;
        this.scheduler = scheduler;
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
        //向主机发送验证信息
        AuthenticateInfoRequest auth = AuthenticateInfoRequest.newBuilder()
                .setAuthenticateInfo(backupNodeConfig.getMasterPass())
                .build();
        Map<String, String> m = new HashMap<>();
        m.put("pass", backupNodeConfig.getMasterPass());
        Packet packet = Packet.builder()
                .msgType(BACK_NODE_AUTH_REQUEST.code())
                .header(m)
                .build();
        ctx.channel().writeAndFlush(packet);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.info("{} connnect to me success", ctx.channel().remoteAddress());
        //super.channelActive(ctx);

    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        //等待auth完成
        if (msg instanceof Packet) {
            Packet packet = (Packet) msg;
            handle(ctx, packet);
        }
        super.channelRead(ctx, msg);
    }

    public void handle(ChannelHandlerContext ctx, Packet packet) {
        MsgType msgType = MsgType.get(packet.getMsgType());
        switch (msgType) {
            case BACK_NODE_AUTH_RESPONSE:
                handleAuth(ctx, packet);
            case BACK_NODE_FETCH_EDITLOG_RESPONSE:
                handleEditLog(ctx, packet);
            case BACK_NODE_FETCH_FSIMAGE_RESPONSE:
                handleFSImgFetch(ctx, packet);
        }
    }

    private void handleEditLog(ChannelHandlerContext ctx, Packet packet) {
        validate();
    }

    private void handleFSImgFetch(ChannelHandlerContext ctx, Packet packet) {
        validate();


    }

    public void handleAuth(ChannelHandlerContext ctx, Packet packet) {
        Map<String, String> respHeader = packet.getHeader();
        synchronized (this) {
            String result = respHeader.get("result");
            if (result.equals("ok")) {
                notifyAll();
                //检查是否自己有fsimage,如果没有就需要发送去拉取fsimage到本地
                this.scheduler.scheduleOnce(FETCH_FSIMAGE, () -> {
                    //判断是否召集有fsimage,如果没就需要去拉取
                    //内存里是否有数据结构
                    Packet<Object> fetchImgRequest = Packet.builder()
                            .msgType(BACK_NODE_FETCH_FSIMAGE_REQUEST.code())
                            .build();
                    ctx.channel().writeAndFlush(fetchImgRequest);
                });

                this.scheduler.schedule(FETCH_EDITLOG, () -> {

                }, 0, 10, TimeUnit.SECONDS); //每10s同步
            }
        }
    }

    public void validate() {
        synchronized (this) {
            if (authStatus == AUTH_INIT) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    log.error("error occur in wait", e);
                    // e.printStackTrace();
                }
            }
        }
    }
}
