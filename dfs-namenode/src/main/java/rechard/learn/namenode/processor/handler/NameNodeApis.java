package rechard.learn.namenode.processor.handler;

import io.netty.channel.ChannelHandlerContext;
import rechard.learn.namenode.constant.MsgType;
import rechard.learn.namenode.protocol.Packet;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * @author Rechard
 **/
public class NameNodeApis {

    public void process(Packet p, ChannelHandlerContext ctx) {
        MsgType msgType = MsgType.get(p.getMsgType());
        Method[] declaredMethods = this.getClass().getDeclaredMethods();
        for (Method m : declaredMethods) {
            PacketHandler ph = m.getAnnotation(PacketHandler.class);
            if (ph != null && ph.type() == msgType) {
                try {
                    m.invoke(this, p, ctx);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @PacketHandler(type = MsgType.NAME_NODE_PEER_AWARE)
    public void processNamenodeAwares(Packet p, ChannelHandlerContext ctx) {
        byte[] bodyBytes = p.getBody();
        System.out.println(new String(bodyBytes));
    }

}
