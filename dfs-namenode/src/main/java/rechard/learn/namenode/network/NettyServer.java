package rechard.learn.namenode.network;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldPrepender;
import lombok.extern.slf4j.Slf4j;
import rechard.learn.namenode.config.NameNodeConfig;
import rechard.learn.namenode.manager.ControllerManager;
import rechard.learn.namenode.processor.handler.NameNodeApis;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Rechard
 **/
@Slf4j
public class NettyServer {
    NameNodeConfig nameNodeConfig;
    ControllerManager controllerManager;

    public NettyServer(NameNodeConfig nameNodeConfig, ControllerManager controllerManager) {
        this.controllerManager = controllerManager;
        this.nameNodeConfig = nameNodeConfig;
    }

    public void start() {

        BaseChannelInitializer serverChannelInitializer = new BaseChannelInitializer();
        ExecutorService threadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);

        serverChannelInitializer.addHandler(PacketDecoder::new);//客户端使用stringEncoder，则这里StringDecoder解析

        serverChannelInitializer.addHandler(() -> {
            return new NettyServerChannelHandler(threadPool, new NameNodeApis(controllerManager, nameNodeConfig));
        });

        //outbound
        serverChannelInitializer.addHandler(() -> new LengthFieldPrepender(4));
        serverChannelInitializer.addHandler(PacketEncoder::new);

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
                    log.info("server started listen on port {}", nameNodeConfig.getPort());
                } else {
                    log.error("server started failed");
                }
            }).sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
