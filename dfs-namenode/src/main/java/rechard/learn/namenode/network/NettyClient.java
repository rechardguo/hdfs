package rechard.learn.namenode.network;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.concurrent.ExecutorService;

/**
 * @author Rechard
 **/
public class NettyClient {
    private ExecutorService executorService;

    public NettyClient(ExecutorService executorService) {
        this.executorService = executorService;
    }

    /**
     * 连接其他机器
     *
     * @param host
     * @param port
     */
    public ConnectFuture connectAsync(String host, int port) {
        BaseChannelInitializer clientChannelInitializer = new BaseChannelInitializer();
        clientChannelInitializer.addHandler(new ClientChannelHandler());

        Bootstrap bootstrap = new Bootstrap();
        final ConnectFuture future = new ConnectFuture();
        ChannelFuture channelFuture = bootstrap
                .group(new NioEventLoopGroup())
                .channel(NioSocketChannel.class)
                .handler(clientChannelInitializer)
                .connect(host, port);
        channelFuture.addListener(f -> {
            if (f.isSuccess()) {
                System.out.println(String.format("连接到namenode服务器%s:%d成功", host, port));
                future.setDone(true);
                future.setSuccess(true);
            } else {
                System.err.println(String.format("连接namenode服务器%s:%d失败，重试+1", host, port));
                connectAsync(host, port);
            }
        });
        return future;
    }
}
