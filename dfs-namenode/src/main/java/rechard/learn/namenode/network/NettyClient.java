package rechard.learn.namenode.network;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldPrepender;
import lombok.extern.slf4j.Slf4j;
import rechard.learn.dfs.common.network.BaseChannelInitializer;
import rechard.learn.dfs.common.network.ConnectFuture;
import rechard.learn.dfs.common.network.PacketDecoder;
import rechard.learn.dfs.common.network.PacketEncoder;
import rechard.learn.dfs.common.utils.DefaultScheduler;
import rechard.learn.namenode.config.NameNodeConfig;
import rechard.learn.namenode.fs.FSDirectory;
import rechard.learn.namenode.manager.ControllerManager;
import rechard.learn.namenode.processor.handler.NameNodeApis;

/**
 * @author Rechard
 **/
@Slf4j
public class NettyClient {
    private DefaultScheduler scheduler;
    private NameNodeConfig nameNodeConfig;
    private ControllerManager controllerManager;
    private FSDirectory fsDirectory;

    public NettyClient(DefaultScheduler scheduler, NameNodeConfig nameNodeConfig,
                       ControllerManager controllerManager, FSDirectory fsDirectory) {
        this.scheduler = scheduler;
        this.nameNodeConfig = nameNodeConfig;
        this.controllerManager = controllerManager;
        this.fsDirectory = fsDirectory;
    }

    /**
     * 连接其他机器
     *
     * @param host
     * @param port
     */
    public ConnectFuture connectAsync(String host, int port) {
        BaseChannelInitializer clientChannelInitializer = new BaseChannelInitializer();
        //outbound
        clientChannelInitializer.addHandler(() -> new LengthFieldPrepender(4));
        clientChannelInitializer.addHandler(PacketEncoder::new);
        //inbound
        clientChannelInitializer.addHandler(PacketDecoder::new);
        clientChannelInitializer.addHandler(() -> new NettyClientChannelHandler(scheduler, new NameNodeApis(controllerManager, nameNodeConfig, fsDirectory)));

        Bootstrap bootstrap = new Bootstrap();
        final ConnectFuture future = new ConnectFuture();
        ChannelFuture channelFuture = bootstrap
                .group(new NioEventLoopGroup())
                .channel(NioSocketChannel.class)
                .handler(clientChannelInitializer)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT) //堆外连接
                .connect(host, port);
        channelFuture.addListener(f -> {
            if (f.isSuccess()) {
                log.info("connect to namenode server{}:{} success!", host, port);
                //System.out.println(String.format("连接到namenode服务器%s:%d成功", host, port));
                future.setDone(true);
                future.setSuccess(true);
            } else {
                //System.err.println(String.format("连接namenode服务器%s:%d失败，重试+1", host, port));
                log.error("connect to namenode server{}:{} fail!", host, port);
                connectAsync(host, port);
            }
        });
        return future;
    }
}
