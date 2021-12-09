package rechard.learn.namenode.fs;

import cn.hutool.core.lang.Assert;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * length(4)+maxtxid(8)+maxtxidoffset(4)+[operation]...[operation]
 * operation=length+[protobuf 系列化]
 *
 * @author Rechard
 **/
public class OperationLog {
    private String storeDir;

    private MappedByteBuffer mappedByteBuffer;
    private int position;
    private Lock lock = new ReentrantLock();
    private Long TXID_HOLDER = -1L;
    private int LENGTH_HOLDER = -1;
    private int LASTMSG_OFFSET_HOLDER = -1;
    private long beginTxid;
    private long currentTxid;

    public OperationLog(String storeDir) {
        this.storeDir = storeDir;
    }

    public OperationLog(MappedByteBuffer mappedByteBuffer) throws InvalidProtocolBufferException {
        check(mappedByteBuffer);
    }

    // 检查文件是否是完整的日志文件
    private void check(MappedByteBuffer mappedByteBuffer) throws InvalidProtocolBufferException {
        int length = mappedByteBuffer.getInt();
        long maxTxid = mappedByteBuffer.getLong();
        //定位到最后1条记录
        int lastTxOffset = mappedByteBuffer.getInt();
        mappedByteBuffer.position(lastTxOffset);
        int lastTxLength = mappedByteBuffer.get();
        byte[] lastTxBytes = new byte[lastTxLength];
        mappedByteBuffer.get(lastTxBytes);
        com.ruyuan.dfs.model.backup.EditLog editLog = com.ruyuan.dfs.model.backup.EditLog.parseFrom(lastTxBytes);
        if (maxTxid == editLog.getTxId()) {
            this.beginTxid = maxTxid;
            this.mappedByteBuffer = mappedByteBuffer;
        }
    }

    public void createEditLog(long txId) throws IOException {
        createEditLog(txId, 1024 * 1024); //默认是1m
    }

    public void createEditLog(long txId, int logFileSize) throws IOException {
        Assert.isTrue(logFileSize <= 1024 * 1024 * 10, "editlog file size can not more than 10M");//最大

        RandomAccessFile file = new RandomAccessFile(storeDir + File.separator + txId, "rw");
        mappedByteBuffer = file.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, logFileSize);//1m
        //长度
        mappedByteBuffer.putInt(LENGTH_HOLDER);
        //最大ID
        mappedByteBuffer.putLong(TXID_HOLDER);
        //最后1条txid偏移位置
        mappedByteBuffer.putInt(LASTMSG_OFFSET_HOLDER);
        this.beginTxid = txId;
    }

    public static OperationLog loadLogFile(String logFile) throws IOException {
        RandomAccessFile file = new RandomAccessFile(logFile, "r");
        MappedByteBuffer mappedByteBuffer = file.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, file.length());
        OperationLog log = new OperationLog(mappedByteBuffer);
        String beginTxid = logFile.substring(logFile.lastIndexOf(File.separator) + 1);
        log.beginTxid = Long.parseLong(beginTxid);
        return log;
    }

    public void saveLog(EditLog log) throws IOException {
        com.ruyuan.dfs.model.backup.EditLog editLog = com.ruyuan.dfs.model.backup.EditLog.newBuilder()
                .setTxId(log.getTxId())
                .setOpType(log.getOpType())
                .putAllAttr(log.getAttr())
                .setPath(log.getPath())
                .build();
        byte[] logBytes = editLog.toByteArray();
        lock.lock();
        //拼接log
        ByteBuffer buffer = ByteBuffer.allocate(4 + logBytes.length);
        buffer.putInt(logBytes.length);
        buffer.put(logBytes);
        buffer.flip();
        System.out.println(buffer.capacity());
        //检查是否文件超过大小
        if (mappedByteBuffer.remaining() >= buffer.capacity()) {
            saveLog(buffer, log.getTxId());
        } else {
            //文件不满1M怎么办？
            //重新定位到7,写入长度
            mappedByteBuffer.position(0);
            mappedByteBuffer.putInt(mappedByteBuffer.limit());
            mappedByteBuffer.putLong(currentTxid);
            mappedByteBuffer.putInt(position);
            mappedByteBuffer.force(); //同步刷盘
            //替换成另个
            createEditLog(log.getTxId());
            saveLog(buffer, log.getTxId());
        }
        lock.unlock();
    }

    private void saveLog(ByteBuffer buffer, long txId) {
        lock.lock();
        if (mappedByteBuffer.remaining() >= buffer.capacity()) {
            mappedByteBuffer.put(buffer);
            mappedByteBuffer.force(); //同步刷盘
            currentTxid = txId;
            position += buffer.capacity();
        }
        lock.unlock();
    }
}
