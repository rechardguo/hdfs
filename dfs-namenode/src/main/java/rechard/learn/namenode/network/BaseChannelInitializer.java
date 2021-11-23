package rechard.learn.namenode.network;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;

import java.util.ArrayList;

/**
 * @author Rechard
 **/
public class BaseChannelInitializer extends ChannelInitializer {

    private java.util.List<ChannelHandler> handlers = new ArrayList<>();

    @Override
    protected void initChannel(Channel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        for (ChannelHandler handler : handlers) {
            pipeline.addLast(handler);
        }

    }

    public void addHandler(ChannelHandler handler) {
        this.handlers.add(handler);
    }
}
