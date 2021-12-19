package rechard.learn.dfs.common.network.file;

import io.netty.channel.socket.SocketChannel;
import lombok.extern.slf4j.Slf4j;
import rechard.learn.dfs.common.constant.MsgType;
import rechard.learn.dfs.common.network.Packet;
import rechard.learn.dfs.common.utils.StringUtils;

import java.io.IOException;

/**
 * @author Rechard
 **/
@Slf4j
public class FSImgSendTask {

    private SocketChannel socketChannel;
    private String filename;
    private FileAttribute fileAttribute;
    //  private OnProgressListener listener;
    private byte[] data;

    /**
     * 用于fsimage的传输
     *
     * @param fis
     * @param socketChannel
     * @param listener
     * @throws IOException
     */
    public FSImgSendTask(byte[] data, String filename, SocketChannel socketChannel) {
        this.data = data;
        this.socketChannel = socketChannel;
        this.fileAttribute = new FileAttribute();
        this.fileAttribute.setFileName(filename);
        this.fileAttribute.setSize(this.data.length);
        this.fileAttribute.setId(StringUtils.getRandomString(12));
        this.fileAttribute.setMd5(StringUtils.md5(data));
    }

    /**
     * @param sync 是否同步执行
     */
    public void execute(boolean sync) {
        try {
            if (data == null || data.length == 0) {
                log.error("fsimage content is empty");
                return;
            }
            if (log.isDebugEnabled()) {
                log.debug("send fsimage header attribute:{}", fileAttribute);
            }
            FilePacket headPackage = FilePacket.builder()
                    .type(FilePacket.HEAD)
                    .fileMetaData(fileAttribute.getAttr())
                    .build();
            Packet packet = Packet.builder().msgType(MsgType.TRANSFER_FILE.code())
                    .body(headPackage.toBytes())
                    .build();
            sendPackage(packet, sync);
            //ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);//1k
            //int len=0;
            //int readLength = 0;
            //文件分段上传
            //while (len<data.length) {
            //byte[] data = new byte[len];
            byte[] content = FilePacket.builder()
                    .type(FilePacket.BODY)
                    .fileMetaData(fileAttribute.getAttr())
                    .body(data)
                    .build().toBytes();

            packet = Packet.builder().msgType(MsgType.TRANSFER_FILE.code())
                    .body(content)
                    .build();

            sendPackage(packet, sync);
            // buffer.clear();
            // readLength += len;
            //float progress = new BigDecimal(String.valueOf(readLength)).multiply(new BigDecimal(100))
            //        .divide(new BigDecimal(String.valueOf(fileAttribute.getSize())), 2, RoundingMode.HALF_UP).floatValue();
            if (log.isDebugEnabled()) {
                log.debug("发送文件包，filename = {}, size={}", filename, data.length);
            }
//                if (listener != null) {
//                    listener.onProgress(fileAttribute.getSize(), readLength, progress, len);
//                }
            //  }
            FilePacket tailPackage = FilePacket.builder()
                    .type(FilePacket.TAIL)
                    .fileMetaData(fileAttribute.getAttr())
                    .build();
            // nettyPacket = NettyPacket.buildPacket(tailPackage.toBytes(), PacketType.TRANSFER_FILE);
            //如果在写入的过程中dataNode挂了？
            //此时会怎么样
            //肯定会上传1半数据到dataNode里，但是由于未完成所以文件并未建立
            packet = Packet.builder().msgType(MsgType.TRANSFER_FILE.code())
                    .body(tailPackage.toBytes())
                    .build();
            sendPackage(packet, sync);
            if (log.isDebugEnabled()) {
                log.debug("发送文件完毕，filename = {}", filename);
            }
//            if (listener != null) {
//                listener.onCompleted();
//            }
        } catch (Exception e) {
            log.error("文件发送失败：", e);
        }
    }

    //force表示是否同步发送，我觉得命名用sync更合适
    private void sendPackage(Packet nettyPacket, boolean sync) throws InterruptedException {
        if (sync) {
            socketChannel.writeAndFlush(nettyPacket).sync();
        } else {
            socketChannel.writeAndFlush(nettyPacket);
        }
    }
}
