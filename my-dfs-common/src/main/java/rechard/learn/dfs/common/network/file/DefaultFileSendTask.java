package rechard.learn.dfs.common.network.file;

import io.netty.channel.socket.SocketChannel;
import lombok.extern.slf4j.Slf4j;
import rechard.learn.dfs.common.constant.MsgType;
import rechard.learn.dfs.common.network.Packet;
import rechard.learn.dfs.common.utils.FileUtil;
import rechard.learn.dfs.common.utils.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 文件发送
 * 1.  发送文件头
 * 2.  多段发送
 * 3.  发送文件尾
 *
 * @author Rechard
 **/
@Slf4j
public class DefaultFileSendTask {

    private SocketChannel socketChannel;
    private String filename;
    private File file;
    private FileAttribute fileAttribute;
    private OnProgressListener listener;
    private byte[] data;

    public DefaultFileSendTask(File file, String filename, SocketChannel socketChannel,
                               OnProgressListener listener) throws IOException {
        this.file = file;
        this.filename = filename;
        this.socketChannel = socketChannel;
        this.fileAttribute = new FileAttribute();
        this.fileAttribute.setFileName(filename);
        this.fileAttribute.setSize(file.length());
        this.fileAttribute.setId(StringUtils.getRandomString(12));
        this.fileAttribute.setMd5(FileUtil.fileMd5(file.getAbsolutePath()));
        this.listener = listener;
    }

    /**
     * 执行逻辑
     */
    public void execute(boolean force) {
        try {
            if (!file.exists()) {
                log.error("文件不存在：[filename={}, localFile={}]", filename, file.getAbsolutePath());
                return;
            }
            RandomAccessFile raf = new RandomAccessFile(file.getAbsoluteFile(), "r");
            FileInputStream fis = new FileInputStream(raf.getFD());
            FileChannel fileChannel = fis.getChannel();
            if (log.isDebugEnabled()) {
                log.debug("发送文件头：{}", filename);
            }
            FilePacket headPackage = FilePacket.builder()
                    .type(FilePacket.HEAD)
                    .fileMetaData(fileAttribute.getAttr())
                    .build();
            Packet packet = Packet.builder().msgType(MsgType.TRANSFER_FILE.code())
                    .body(headPackage.toBytes())
                    .build();
            sendPackage(packet, force);
            ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);//1k
            int len;
            int readLength = 0;
            //文件分段上传
            while ((len = fileChannel.read(buffer)) > 0) {
                buffer.flip();
                byte[] data = new byte[len];
                buffer.get(data);
                byte[] content = FilePacket.builder()
                        .type(FilePacket.BODY)
                        .fileMetaData(fileAttribute.getAttr())
                        .body(data)
                        .build().toBytes();

                packet = Packet.builder().msgType(MsgType.TRANSFER_FILE.code())
                        .body(content)
                        .build();

                sendPackage(packet, force);
                buffer.clear();
                readLength += len;
                float progress = new BigDecimal(String.valueOf(readLength)).multiply(new BigDecimal(100))
                        .divide(new BigDecimal(String.valueOf(fileAttribute.getSize())), 2, RoundingMode.HALF_UP).floatValue();
                if (log.isDebugEnabled()) {
                    log.debug("发送文件包，filename = {}, size={}, progress={}", filename, data.length, progress);
                }
                if (listener != null) {
                    listener.onProgress(fileAttribute.getSize(), readLength, progress, len);
                }
            }
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
            sendPackage(packet, force);
            if (log.isDebugEnabled()) {
                log.debug("发送文件完毕，filename = {}", filename);
            }
            if (listener != null) {
                listener.onCompleted();
            }
        } catch (Exception e) {
            log.error("文件发送失败：", e);
        }
    }

    //force表示是否同步发送，我觉得命名用sync更合适
    private void sendPackage(Packet nettyPacket, boolean force) throws InterruptedException {
        if (force) {
            socketChannel.writeAndFlush(nettyPacket).sync();
        } else {
            socketChannel.writeAndFlush(nettyPacket);
        }
    }
}