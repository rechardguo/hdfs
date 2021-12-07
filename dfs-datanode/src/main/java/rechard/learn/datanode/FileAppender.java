package rechard.learn.datanode;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author Rechard
 **/
public class FileAppender {

    private File file;
    // private FileInputStream fis;
    private FileChannel channel;
    private MappedByteBuffer writeBuffer;

    public FileAppender(File file, long fileSize) throws IOException {
        this.file = file;
        //this.fis = new FileInputStream(this.file);
        RandomAccessFile randomAccessFile = new RandomAccessFile(this.file, "rw");
        this.channel = randomAccessFile.getChannel();
        this.writeBuffer = this.channel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
    }

    public void append(byte[] data) {
        this.writeBuffer.put(data);
    }

    public void flush() {
        this.writeBuffer.force();
    }
}
