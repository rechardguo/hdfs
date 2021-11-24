package rechard.learn.namenode.network;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;

import java.util.ArrayList;
import java.util.function.Supplier;

/**
 * @author Rechard
 **/
public class BaseChannelInitializer extends ChannelInitializer {

    private java.util.List<Supplier<ChannelHandler>> handlerSuppliers = new ArrayList<>();

    @Override
    protected void initChannel(Channel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        for (Supplier<ChannelHandler> handlerSupplier : handlerSuppliers) {
            pipeline.addLast(handlerSupplier.get());
        }
    }

    public void addHandler(Supplier<ChannelHandler> supplier) {
        handlerSuppliers.add(supplier);
    }

//    public void addHandler(ChannelHandler handler) {
//        this.handlers.add(handler);
//    }
}
