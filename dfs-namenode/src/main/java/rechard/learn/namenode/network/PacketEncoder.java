package rechard.learn.namenode.network;

import com.ruyuan.dfs.model.common.NettyPacketHeader;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import lombok.extern.slf4j.Slf4j;
import rechard.learn.namenode.protocol.Packet;

import java.util.List;

/**
 * 将packet进行encode
 *
 * @author Rechard
 **/
@Slf4j
public class PacketEncoder extends MessageToMessageEncoder<Packet> {
    private static final int MSGTYPE_LENGTH = 4;
    private static final int HEADER_LENGTH = 4;
    private static final int BODY_LENGTH = 4;

    @Override
    protected void encode(ChannelHandlerContext ctx, Packet msg, List<Object> out)
            throws Exception {
        //写入header
        byte[] headerBytes = null;
        if (msg.getHeader() != null) {
            NettyPacketHeader header = NettyPacketHeader.newBuilder().putAllHeaders(msg.getHeader()).build();
            headerBytes = header.toByteArray();
        }
        ByteBuf byteBuf = null;
        try {
            byteBuf = ctx.alloc().heapBuffer(
                    MSGTYPE_LENGTH
                            + HEADER_LENGTH
                            + (headerBytes != null ? headerBytes.length : 0)
                            + BODY_LENGTH
                            + msg.getBody().length);
            byteBuf.writeInt(msg.getMsgType());
            if (headerBytes == null) {
                byteBuf.writeInt(0);
            } else {
                byteBuf.writeInt(headerBytes.length);
                byteBuf.writeBytes(headerBytes);
            }
            byteBuf.writeInt(msg.getBody().length);
            byteBuf.writeBytes(msg.getBody());
            out.add(byteBuf);
        } catch (Exception e) {
            log.error("error in encode packet", e);
            if (byteBuf != null) {
                byteBuf.release();
            }
        }
    }
}
