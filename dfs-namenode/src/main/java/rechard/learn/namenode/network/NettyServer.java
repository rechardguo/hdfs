package rechard.learn.namenode.network;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import rechard.learn.namenode.config.NameNodeConfig;

/**
 * @author Rechard
 **/
public class NettyServer {
    NameNodeConfig nameNodeConfig;

    public NettyServer(NameNodeConfig nameNodeConfig) {
        this.nameNodeConfig = nameNodeConfig;
    }

    public void start() {
        BaseChannelInitializer serverChannelInitializer = new BaseChannelInitializer();
        serverChannelInitializer.addHandler(new NettyServerChannelHandler());

        ServerBootstrap serverBootstrap = new ServerBootstrap();
        NioEventLoopGroup boss = new NioEventLoopGroup();
        NioEventLoopGroup worker = new NioEventLoopGroup();
        serverBootstrap.group(boss, worker)
                .channel(NioServerSocketChannel.class)
                .childHandler(serverChannelInitializer);
        ChannelFuture channelFuture = serverBootstrap.bind(nameNodeConfig.getPort());
        try {
            channelFuture.addListener(future -> {
                if (future.isSuccess()) {
                    System.out.println(String.format("服务器启动成功，port=%d", nameNodeConfig.getPort()));
                } else {
                    System.err.println(String.format("服务器启动失败"));
                }
            }).sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
