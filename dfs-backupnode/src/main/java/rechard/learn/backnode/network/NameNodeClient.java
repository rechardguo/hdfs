package rechard.learn.backnode.network;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldPrepender;
import lombok.extern.slf4j.Slf4j;
import rechard.learn.backnode.config.BackupNodeConfig;
import rechard.learn.dfs.common.network.BaseChannelInitializer;
import rechard.learn.dfs.common.network.PacketDecoder;
import rechard.learn.dfs.common.network.PacketEncoder;
import rechard.learn.dfs.common.utils.DefaultScheduler;

/**
 * 连接namenode的client类
 *
 * @author Rechard
 **/
@Slf4j
public class NameNodeClient {
    private BackupNodeConfig backupNodeConfig;
    private DefaultScheduler scheduler;

    public NameNodeClient(BackupNodeConfig backupNodeConfig, DefaultScheduler scheduler) {
        this.backupNodeConfig = backupNodeConfig;
        this.scheduler = scheduler;
    }

    /**
     * 连接master
     *
     * @param host
     * @param port
     */
    public void connectMaster() {
        String host = backupNodeConfig.getMasterIp();
        int port = backupNodeConfig.getMasterPort();
        BaseChannelInitializer clientChannelInitializer = new BaseChannelInitializer();
        //outbound
        clientChannelInitializer.addHandler(() -> new LengthFieldPrepender(4));
        clientChannelInitializer.addHandler(PacketEncoder::new);
        //inbound
        clientChannelInitializer.addHandler(PacketDecoder::new);
        clientChannelInitializer.addHandler(() -> new NameNodeClientChannelHandler(backupNodeConfig, scheduler));

        Bootstrap bootstrap = new Bootstrap();
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();
        ChannelFuture channelFuture = bootstrap
                .group(new NioEventLoopGroup())
                .channel(NioSocketChannel.class)
                .handler(clientChannelInitializer)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT) //堆外连接
                .connect(host, port);
        try {
            channelFuture.addListener(f -> {
                if (f.isSuccess()) {
                    log.info("connect to namenode server{}:{} success!", host, port);
                } else {
                    log.error("connect to namenode server{}:{} fail!", host, port);
                }
            });
            // Wait untdil the connection is closed.
            channelFuture.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            log.error("connect to namenode server{}:{} fail!", host, port);
            workerGroup.shutdownGracefully();
        }
    }
}
