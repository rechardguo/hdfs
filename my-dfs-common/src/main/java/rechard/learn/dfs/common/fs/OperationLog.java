package rechard.learn.dfs.common.fs;

import cn.hutool.core.lang.Assert;
import com.google.protobuf.InvalidProtocolBufferException;
import com.ruyuan.dfs.model.backup.EditLog;
import lombok.extern.slf4j.Slf4j;
import rechard.learn.dfs.common.exeception.NameNodeException;
import rechard.learn.dfs.common.utils.FileUtil;

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
@Slf4j
public class OperationLog {
    private String storeDir;

    private MappedByteBuffer mappedByteBuffer;
    private int position; //writeposition
    private Lock lock = new ReentrantLock();
    private Long TXID_HOLDER = -1L;
    private int LENGTH_HOLDER = -1;
    private int LASTMSG_OFFSET_HOLDER = -1;
    //private long beginTxid;
    private long currentTxid;
    private File file;
    private final static int BLANK_MAGIC_CODE = -875286124;
    private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4;

    public OperationLog(File file) throws IOException {
        this.file = file;
        this.currentTxid = Long.parseLong(file.getName());//文件名就是开始的txid
        this.storeDir = file.getParent();
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
        this.mappedByteBuffer = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, file.length());
    }

    //大于startTxid的记录才会回放
    //检查并回放到directory里
    public boolean checkAndreplay(FSDirectory directory, long startTxid) {
        if (!checkWholeFile(mappedByteBuffer))
            return false;
        while (mappedByteBuffer.hasRemaining()) {
            int eLoglength = mappedByteBuffer.getInt();
            byte[] elogData = new byte[eLoglength - 4 - 4];
            mappedByteBuffer.get(elogData);
            int calcrc32 = FileUtil.crc32(elogData);
            int crc32 = mappedByteBuffer.getInt(4);
            if (calcrc32 == crc32) {
                try {
                    EditLog editLog = EditLog.parseFrom(elogData);
                    //只有比fsimage大的txidi才进行回放
                    if (editLog.getTxId() > startTxid)
                        directory.replay(editLog);
                    this.currentTxid = editLog.getTxId();
                } catch (InvalidProtocolBufferException | NameNodeException e) {
                    log.info("crc check not pass for file {},position {}", this.file.getName(), this.position);
                    return false;
                }
            }
        }
        return true;
    }

    //editlog editlog blank 空白数+magiccode
    // 1.检查文件是否符合文件尾 是否有magiccode
    // 2.拿到空白值定位到最后
    private boolean checkWholeFile(MappedByteBuffer mappedByteBuffer) {
        ByteBuffer byteBuffer = mappedByteBuffer.slice();
        //重新定位到最后
        byteBuffer.position(byteBuffer.capacity() - 8);
        int maxBlankSize = byteBuffer.getInt();
        int magicCode = byteBuffer.getInt();
        if (magicCode != BLANK_MAGIC_CODE) {
            return false;
        }
        //定位到最后
        mappedByteBuffer.position(mappedByteBuffer.capacity() - maxBlankSize - 4 - 4);//定位到最后的位置
        mappedByteBuffer.slice();
        return true;
//
//        int lastTxLength = mappedByteBuffer.get();
//        byte[] lastTxBytes = new byte[lastTxLength];
//        mappedByteBuffer.get(lastTxBytes);
//        com.ruyuan.dfs.model.backup.EditLog editLog = com.ruyuan.dfs.model.backup.EditLog.parseFrom(lastTxBytes);
//        if (maxTxid == editLog.getTxId()) {
//            this.beginTxid = maxTxid;
//            this.mappedByteBuffer = mappedByteBuffer;
//        }
    }

    public OperationLog createOperationLog(long txId) throws IOException {
        return createOperationLog(txId, 1024 * 1024); //默认是1m
    }

    public OperationLog createOperationLog(long txId, int logFileSize) throws IOException {
        Assert.isTrue(logFileSize <= 1024 * 1024 * 10, "editlog file size can not more than 10M");//最大
        File file = new File(storeDir + File.separator + txId);
        // RandomAccessFile accessFile = new RandomAccessFile(this.file, "rw");
        //mappedByteBuffer = accessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, logFileSize);//1m
        //长度
        //mappedByteBuffer.putInt(LENGTH_HOLDER);
        //最大ID
        //mappedByteBuffer.putLong(TXID_HOLDER);
        //最后1条txid偏移位置
        //mappedByteBuffer.putInt(LASTMSG_OFFSET_HOLDER);
        //this.position=0;
        //this.beginTxid = txId;
        return new OperationLog(file);
    }

//    public static OperationLog loadLogFile(String logFile) throws IOException {
//        RandomAccessFile file = new RandomAccessFile(logFile, "r");
//        MappedByteBuffer mappedByteBuffer = file.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, file.length());
//        OperationLog log = new OperationLog(mappedByteBuffer);
//        String beginTxid = logFile.substring(logFile.lastIndexOf(File.separator) + 1);
//        log.beginTxid = Long.parseLong(beginTxid);
//        return log;
//    }

    public OperationSaveResult saveLog(OperationLogItem log) throws IOException {
        OperationSaveResult operationSaveResult = new OperationSaveResult();
        EditLog editLog = EditLog.newBuilder()
                .setTxId(log.getTxId())
                .setOpType(log.getOpType())
                .putAllAttr(log.getAttr())
                .setPath(log.getPath())
                .build();
        byte[] logBytes = editLog.toByteArray();
        lock.lock();
        int crc32 = FileUtil.crc32(logBytes);
        //拼接log
        ByteBuffer buffer = ByteBuffer.allocate(4 + logBytes.length + 4); //msg长+msgbody+crc长
        buffer.putInt(logBytes.length);//长度包含了crc长度
        buffer.put(logBytes);
        buffer.putInt(crc32);
        buffer.flip();
        // System.out.println(buffer.capacity());
        //检查是否文件超过大小
        if (mappedByteBuffer.remaining() > buffer.capacity() + END_FILE_MIN_BLANK_LENGTH) {
            saveLog(buffer, log.getTxId());
        } else {
            mappedByteBuffer.putInt(mappedByteBuffer.remaining());//最后4位是空闲
            //mappedByteBuffer.force(); //同步刷盘
            //替换成另个
            OperationLog operationLog = createOperationLog(log.getTxId());
            operationLog.saveLog(buffer, log.getTxId());
            operationSaveResult.setState(OperationSaveResult.CREATE_NEW_FILE_AND_PUT_OK);
            operationSaveResult.setOperationLog(operationLog);
        }
        lock.unlock();
        return operationSaveResult;
    }

    private void saveLog(ByteBuffer buffer, long txId) {
        lock.lock();
        if (mappedByteBuffer.remaining() >= buffer.capacity()) {
            mappedByteBuffer.put(buffer);
            // mappedByteBuffer.force(); //同步刷盘
            currentTxid = txId;
            position += buffer.capacity();
        }
        lock.unlock();
    }

    public void flush() {
        mappedByteBuffer.force();
    }

    /**
     * @return 返回当前的记录到的txid
     */
    public long getCurrentTxid() {
        return this.currentTxid;
    }

}
