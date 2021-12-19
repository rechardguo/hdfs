package rechard.learn.dfs.common.network;

import com.ruyuan.dfs.model.common.NettyPacketHeader;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;

import static rechard.learn.dfs.common.constant.NameNodeConstant.MAX_MSG_LENGTH;


/**
 * @author Rechard
 **/
@Slf4j
public class PacketDecoder extends LengthFieldBasedFrameDecoder {
    private static final int MSGTYPE_LENGTH = 4;
    private static final int HEADER_LENGTH = 4;
    private static final int BODY_LENGTH = 4;

    //通过协议拆分包后得到
    public PacketDecoder() {
        super(MAX_MSG_LENGTH, 0,
                4, 0, 4);
    }

    @Override
    public Object decode(ChannelHandlerContext ctx, ByteBuf byteBuf) throws Exception {
        ByteBuf msg = (ByteBuf) super.decode(ctx, byteBuf);
        if (msg != null) {
            try {
                int msgType = msg.readInt();
                int headerLength = msg.readInt();
                NettyPacketHeader nettyPacketHeader = null;
                if (headerLength > 0) {
                    // ByteBuf headerByteBuf = msg.readBytes(headerLength);
                    byte[] headerBytes = new byte[headerLength];
                    nettyPacketHeader = NettyPacketHeader.parseFrom(headerBytes);
                }
                int bodyLength = msg.readInt();
                byte[] bodyBytes = new byte[bodyLength];
                msg.readBytes(bodyBytes);
                Packet<Object> packet = Packet.builder()
                        .msgType(msgType)
                        .header(nettyPacketHeader == null ? null : nettyPacketHeader.getHeadersMap())
                        .body(bodyBytes)
                        .build();
                return packet;
            } catch (Exception e) {
                log.error("error decode message", e);
            } finally {
                ReferenceCountUtil.release(byteBuf);
            }
        }
        return null;
    }
}
