package rechard.learn.dfs;

import com.ruyuan.dfs.model.client.MkdirRequest;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldPrepender;
import rechard.learn.dfs.callback.DefaultMessageCallback;
import rechard.learn.dfs.callback.MessageCallback;
import rechard.learn.dfs.common.constant.MsgType;
import rechard.learn.dfs.common.network.Packet;
import rechard.learn.dfs.common.network.PacketDecoder;
import rechard.learn.dfs.common.network.PacketEncoder;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;


/**
 * @author Rechard
 **/
public class FileSystemClient {

    private volatile Channel channel;
    private AtomicLong seqProducer = new AtomicLong();
    private String uid;
    private String token;
    private MessageCallback DEFAULT_MSGCALLBACK = new DefaultMessageCallback();
    private MessageCallback callback = DEFAULT_MSGCALLBACK;
    private Bootstrap bootstrap;

    public FileSystemClient(String uid, String token) {
        this.uid = uid;
        this.token = token;
    }

    public void setCallback(MessageCallback callback) {
        this.callback = callback;
    }

    public void connect(String host, int port) {
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        bootstrap = new Bootstrap();
        bootstrap.group(workerGroup);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline()
                        .addLast(new LengthFieldPrepender(4))
                        .addLast(new PacketEncoder())
                        .addLast(new PacketDecoder())
                        .addLast(new FileSystemClientHandler(callback));
            }
        });
        try {
            final ChannelFuture future = bootstrap.connect(host, port).sync();
            //使用了future
            future.addListener(f -> {
                if (f.isSuccess()) {
                    this.channel = future.channel();
                    System.out.println("连接到服务器成功");
                }
            });

            future.channel().closeFuture().addListener(future1 -> {
                //优雅的关闭
                workerGroup.shutdownGracefully();
            });

            ensureConnect();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void ensureConnect() {
        while (channel == null) {
        }
    }

    /**
     * 发送命令，例如
     * mkdir /a/b/c
     * deldir /a/b/c
     *
     * @param command
     * @param path
     */
    public void send(String command, String path, Map<String, String> attr) {
        Packet packet = null;
        if (command.equalsIgnoreCase("mkdir")) {
            MkdirRequest request = MkdirRequest.newBuilder()
                    .setPath(path)
                    .putAllAttr(attr)
                    .build();
            packet = Packet.builder()
                    .msgType(MsgType.FS_OP_MKDIR_REQUEST.code())
                    .body(request.toByteArray())
                    .build();
        }
        this.channel.writeAndFlush(packet);
    }


}
